import sys, os, logging
import joblib
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from spark_streaming.schema import TRANSACTION_SCHEMA
from dotenv import load_dotenv

# Load env variables for secure access
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER   = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC    = 'transactions'
CHECKPOINT_DIR = '/tmp/spark_checkpoints/transactions'
MONGO_URI      = os.environ.get('MONGO_URI', 'mongodb://127.0.0.1:27017')
MODEL_DIR      = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'anomaly_detection')

def predict_anomaly(amount_s: pd.Series, hour_s: pd.Series, is_online_s: pd.Series, lat_s: pd.Series, lon_s: pd.Series) -> pd.Series:
    """
    Pandas UDF for ML Inference.
    Executes on worker nodes directly leveraging Apache Arrow.
    """
    # Load model from disk per task execution context
    model = joblib.load(os.path.join(MODEL_DIR, 'isolation_forest_model.pkl'))
    scaler = joblib.load(os.path.join(MODEL_DIR, 'scaler.pkl'))
    
    # Structure dataframe identical to training phase
    df = pd.DataFrame({
        'amount': amount_s, 
        'hour': hour_s, 
        'is_online_num': is_online_s, 
        'latitude': lat_s, 
        'longitude': lon_s
    })
    
    # Scale & Predict
    X_scaled = scaler.transform(df)
    anomaly_scores = model.decision_function(X_scaled)
    
    # Return scores (-1 typically anomalous for Iso Forest, but decision function returns real values, <0 is anomaly)
    return pd.Series(anomaly_scores)

def create_spark():
    master_url = os.environ.get('SPARK_MASTER_URL', 'spark://localhost:7077')
    driver_ip = master_url.split('://')[1].split(':')[0] if '://' in master_url else 'localhost'
    
    return (SparkSession.builder
        .appName('CreditCardAnomalyDetection')
        .master(master_url)
        .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,'
            'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') # Required for Pandas UDF efficiency
        .config('spark.driver.bindAddress', '0.0.0.0') # Listen on all interfaces
        .config('spark.driver.host', driver_ip)        # Broadcast Laptop 1 IP
        .config('spark.driver.memory',      '1g')
        .config('spark.executor.memory',    '1g')
        .getOrCreate())

def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel('WARN')
    logger.info(f'Spark {spark.version} ready for TF ML Pipeline')
    
    # Register the Pandas UDF for scalar logic
    predict_udf = F.pandas_udf(predict_anomaly, returnType=DoubleType())

    # 1. Read from Kafka
    raw = (spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', KAFKA_BROKER)
        .option('subscribe', KAFKA_TOPIC)
        .option('startingOffsets', 'latest')
        .option('maxOffsetsPerTrigger', 500)
        .load())

    # 2. Parse JSON & Add Basic Temporal columns
    parsed = (raw
        .selectExpr('CAST(value AS STRING) as json_str')
        .select(F.from_json(F.col('json_str'), TRANSACTION_SCHEMA).alias('d'))
        .select('d.*')
        .withColumn('processing_time', F.current_timestamp())
        .withColumn('event_time', F.to_timestamp(F.col('timestamp'))))

    # 3. Feature Engineering for the Pipeline
    featured = (parsed
                .withColumn('hour', F.hour(F.col('event_time')))
                .withColumn('is_online_num', F.col('is_online').cast(IntegerType())))

    # 4. Score Batch & Detect Anomalies
    scored = (featured.withColumn('anomaly_score', 
                    predict_udf('amount', 'hour', 'is_online_num', 'latitude', 'longitude'))
             # IsolationForest: < 0 is an outlier
             .withColumn('rule_anomaly', F.when(F.col('anomaly_score') < 0, True).otherwise(False)))

    # 5. Output Streams

    def write_all_sinks(batch_df, batch_id):
        # Cache the dataframe so we don't recompute the ML model 3 times
        batch_df.persist()
        n = batch_df.count()
        if n == 0: 
            batch_df.unpersist()
            return
            
        logger.info(f'════════ Batch {batch_id} Processing {n} records ════════')
        
        # 1. Console Output
        batch_df.show(5, truncate=False)
        
        # 2. MongoDB Output
        final_df = batch_df.drop('hour', 'is_online_num')
        (final_df.write
            .format('mongodb')
            .mode('append')
            .option('uri', MONGO_URI)
            .option('database', 'credit_card_db')
            .option('collection', 'transactions')
            .save())
        logger.info(f'[Batch {batch_id}] MongoDB ✅')


        batch_df.unpersist()

    # Master Unified Stream
    main_query = (scored.writeStream
        .outputMode('append')
        .foreachBatch(write_all_sinks)
        .option('checkpointLocation', CHECKPOINT_DIR + '/unified_pipeline')
        .trigger(processingTime='15 seconds')
        .start())

    logger.info('══════════════════════════════════════')
    logger.info('Isolation Forest Stream Pipeline Active')
    logger.info('══════════════════════════════════════')
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info('Keyboard interrupt received. Gracefully stopping Spark Streaming queries...')
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        logger.info('Spark gracefully shut down.')

if __name__ == '__main__':
    run()
