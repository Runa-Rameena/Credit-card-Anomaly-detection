# 🛠 Technical Architecture & Operational Guide

This document provides a comprehensive technical breakdown of the **Real-Time Credit Card Anomaly Detection Pipeline**, detailing the system components, cluster configuration, multi-node deployment steps, and database schemas.

---

## 🏛 System Architecture Overview

```mermaid
flowchart TD
    subgraph Data Generation & Messaging
        A["Producer: Transaction Simulator<br/>(Faker + JSON Payload Generator)"]
        B["Apache Kafka Broker<br/>(Topic: 'transactions', Partitions: 3)"]
        Z["Apache Zookeeper<br/>(Cluster Manager)"]
        A -->|Produce Messages| B
        Z --- B
    end

    subgraph Distributed Processing Engine
        C["Apache Spark Driver<br/>(Laptop 1 / Master Node)"]
        W1["Spark Worker Node 1<br/>(Laptop 1 Executors)"]
        W2["Spark Worker Node 2<br/>(Laptop 2 Executors over Wi-Fi)"]
        
        B -->|Micro-Batch Read Stream| C
        C -->|Distribute Tasks| W1
        C -->|Distribute Tasks| W2
        
        subgraph Machine Learning Pipeline
            D["Pandas UDF (Apache Arrow)"]
            E["StandardScaler"]
            F["Isolation Forest Model<br/>(n_estimators=100, contamination=0.05)"]
            D --> E --> F
        end
        
        W1 --- Machine Learning Pipeline
        W2 --- Machine Learning Pipeline
    end

    subgraph Data Persistence & UI
        M[("MongoDB Collection<br/>(Operational NoSQL Sink)")]
        S[("Snowflake Data Warehouse<br/>(Analytical Warehouse Sink)")]
        UI["Streamlit Dashboard<br/>(Real-Time Monitoring & Manual Overrides)"]
        
        C -->|foreachBatch Parallel Write| M
        C -->|foreachBatch Parallel Write| S
        
        M -->|Live Document Query| UI
        S -->|Analytical Query| UI
        UI -->|Manual Fraud Injection| B
    end
```

---

## 💻 Multi-Laptop Cluster Deployment Guide

The pipeline natively supports distributed execution across multiple physical computers connected over a local network (Wi-Fi or LAN).

### 🌐 Network Discovery & Environment Setup

#### Step 1: Discover Master IP Address
On **Laptop 1 (Master Node)**, identify the local network IP:
```bash
hostname -I | awk '{print $1}'
```
*(Example output: `192.168.1.15` or `10.52.67.142`)*

Export this IP in every open terminal on Laptop 1:
```bash
export MASTER_IP=$(hostname -I | awk '{print $1}')
```

---

### 🚀 Laptop 1 (Master Node Execution Flow)

Open separate terminal windows for each service:

#### 1️⃣ Terminal 1: Zookeeper
```bash
cd ~/bigdata/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### 2️⃣ Terminal 2: Kafka Broker
```bash
cd ~/bigdata/kafka
bin/kafka-server-start.sh config/server.properties
```

#### 3️⃣ Terminal 3: Kafka Topic Creation (Run Once)
```bash
~/bigdata/kafka/bin/kafka-topics.sh --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

#### 4️⃣ Terminal 4: Start Spark Master & Local Worker
```bash
source ~/projects/credit_card_anomaly/venv/bin/activate
$SPARK_HOME/sbin/start-master.sh --host $MASTER_IP --port 7077
$SPARK_HOME/sbin/start-worker.sh spark://$MASTER_IP:7077
```

#### 5️⃣ Terminal 5: Synthetic Transaction Producer
```bash
cd ~/projects/credit_card_anomaly
source venv/bin/activate
python -m producer.kafka_producer
```

#### 6️⃣ Terminal 6: Spark Structured Streaming ML Job
```bash
cd ~/projects/credit_card_anomaly
source venv/bin/activate
export SPARK_MASTER_URL="spark://$MASTER_IP:7077"
python -m spark_streaming.streaming_job
```

#### 7️⃣ Terminal 7: Streamlit Monitoring Dashboard
```bash
cd ~/projects/credit_card_anomaly
source venv/bin/activate
streamlit run dashboard/app.py
```
*Access the dashboard at `http://<MASTER_IP>:8501`*

---

### 💻 Laptop 2 (Worker Node Setup)

To attach a second laptop as an active compute worker:

1. Connect Laptop 2 to the same Wi-Fi network.
2. Obtain Laptop 1's IP (`<MASTER_IP>`).
3. Execute the following on Laptop 2:

```bash
cd ~/projects/credit_card_anomaly
source venv/bin/activate

# Ensure PySpark version matches Laptop 1 exactly
pip install pyspark==3.5.2

# Connect worker executor to Laptop 1 Spark Master
$SPARK_HOME/sbin/start-worker.sh spark://<MASTER_IP>:7077
```

4. Verify worker connection by opening `http://<MASTER_IP>:8080` in your web browser. Laptop 2 will appear under the **Alive Workers** section.

---

## 🗄 Storage Schemas

### 1. MongoDB Document Schema (`credit_card_db.transactions`)
```json
{
  "transaction_id": "8f3b2a19-94b2-4d10-a4f6-8c291d9b3a01",
  "card_id": "CARD_0014",
  "timestamp": "2026-09-05T16:00:00Z",
  "amount": 14500.50,
  "merchant": "Unknown Online Store",
  "merchant_category": "online",
  "location_city": "New York",
  "location_country": "US",
  "latitude": 40.7128,
  "longitude": -74.0060,
  "device": "desktop",
  "is_online": true,
  "card_present": false,
  "anomaly_score": -0.2854,
  "rule_anomaly": true,
  "processing_time": "2026-09-05T16:00:02.145Z"
}
```

### 2. Snowflake Data Warehouse DDL (`TRANSACTIONS`)
```sql
CREATE TABLE IF NOT EXISTS TRANSACTIONS (
    TRANSACTION_ID VARCHAR(50) PRIMARY KEY,
    CARD_ID VARCHAR(20),
    EVENT_TIME TIMESTAMP_NTZ,
    AMOUNT FLOAT,
    MERCHANT VARCHAR(100),
    MERCHANT_CATEGORY VARCHAR(50),
    LOCATION_CITY VARCHAR(50),
    LOCATION_COUNTRY VARCHAR(10),
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    DEVICE VARCHAR(20),
    IS_ONLINE BOOLEAN,
    CARD_PRESENT BOOLEAN,
    RULE_ANOMALY BOOLEAN,
    ANOMALY_SCORE FLOAT,
    PROCESSING_TIME TIMESTAMP_NTZ
);
```

---

## 🧹 Graceful System Shutdown

To stop all services cleanly on Laptop 1:
```bash
# 1. Stop Kafka & Zookeeper
cd ~/bigdata/kafka
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh

# 2. Stop Spark Cluster
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh
```
