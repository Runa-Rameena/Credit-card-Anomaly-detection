import json, time, random, logging, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka import KafkaProducer
from producer.transaction_generator import generate_transaction

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def run_producer(tps=2):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all', retries=3
    )
    logger.info('Producer connected → topic: transactions')
    count = 0
    try:
        while True:
            txn = generate_transaction()
            producer.send('transactions', value=txn, key=txn['card_id'].encode('utf-8'))
            count += 1
            if count % 10 == 0:
                logger.info(f'[TXN #{count}] {txn["card_id"]} ${txn["amount"]:,.2f} — {txn["merchant"]}')
            time.sleep(max(0.1, 1.0/tps + random.uniform(-0.1, 0.2)))
    except KeyboardInterrupt:
        logger.info(f'Stopped. Sent: {count}')
    finally:
        producer.flush()
        producer.close()

if __name__ == '__main__':
    run_producer(tps=2)
