# 💳 Real-Time Credit Card Anomaly Detection Pipeline

> **Big Data Analytics Project (22AIE312)**  
> **Institution:** Amrita Vishwa Vidyapeetham, Chennai  
> **Course Mentor:** Dr. S. Saravanan (Dept. of Computer Science & Engineering)  
> **Team Members:**  
> - Renuka V J (`CH.SC.U4AIE23057`)  
> - Vaishnavi S (`CH.SC.U4AIE23059`)

---

## 📌 Project Overview

This project presents an enterprise-grade **Real-Time Credit Card Anomaly Detection Pipeline** built to process high-throughput transaction streams and isolate fraudulent activities in real time.

By integrating **Apache Kafka**, **Apache Spark Structured Streaming**, an **Unsupervised Isolation Forest ML Model** optimized via **Pandas UDF (Apache Arrow)**, and dual persistence sinks (**MongoDB** for low-latency operational dashboarding and **Snowflake** for historical analytics), this architecture achieves sub-second fraud detection on distributed hardware.

---

## 🏗 System Architecture

```mermaid
flowchart TD
    A[Transaction Producer<br/>Faker + JSON Stream] -->|Transactions Topic| B[Apache Kafka Broker<br/>+ Zookeeper Cluster]
    B -->|Spark ReadStream| C[Apache Spark<br/>Structured Streaming]
    
    subgraph Spark Core Engine
        C --> D[Feature Engineering<br/>hour, amount, location]
        D --> E[Pandas UDF Inference<br/>PyArrow Accelerated]
        E --> F[Isolation Forest ML Model<br/>decision_function < 0]
    end
    
    F -->|Dual-Sink Write| G[(MongoDB NoSQL)<br/>Real-Time Operational Sink]
    F -->|Dual-Sink Write| H[(Snowflake DW)<br/>Historical Analytical Sink]
    
    G --> I[Streamlit Dashboard<br/>Live KPI & Anomaly Alerts]
    H --> I
    I -->|Manual Override| A
```

---

## ✨ Key Features

- **High-Throughput Ingestion**: Decentralized, fault-tolerant message streaming via **Apache Kafka**.
- **Distributed Machine Learning**: Real-time batch predictions using `sklearn.ensemble.IsolationForest` executed across Spark worker nodes with **Pandas UDFs** and **Apache Arrow** byte-transfer optimization.
- **Dual-Sink Persistence**:
  - **MongoDB**: Instant NoSQL storage backing real-time alerts and Streamlit dashboard polling.
  - **Snowflake**: Data warehouse staging for long-term historical analytics, regulatory audit logs, and retroactive model retraining.
- **Dynamic Multi-Laptop Cluster Support**: Easily scalable across multiple nodes over Wi-Fi/LAN networks.
- **Real-Time Monitoring & Simulation**: Interactive **Streamlit Dashboard** featuring continuous KPI metrics, distribution charts, custom Snowflake SQL query runner, and a manual **Fraud Injection Trigger**.

---

## 📁 Repository Structure

```text
credit_card_anomaly/
├── anomaly_detection/          # Machine learning model training & serialized models
│   ├── train_model.py          # Script to generate synthetic dataset & train Isolation Forest
│   ├── isolation_forest_model.pkl # Serialized Isolation Forest model
│   └── scaler.pkl              # Serialized StandardScaler
├── config/                     # Configuration files & environment templates
│   ├── .env.example            # Environment variables template
│   └── .env                    # Local environment secrets (Git-ignored)
├── dashboard/                  # Streamlit visual dashboard
│   └── app.py                  # Live metrics, charts, manual fraud injection
├── database/                   # Storage integrations
│   └── snowflake_writer.py     # Snowflake connector & batch writer
├── producer/                   # Synthetic transaction producer
│   ├── kafka_producer.py       # Kafka event generator stream
│   └── transaction_generator.py # Synthetic credit card transaction builder
├── spark_streaming/            # Core Spark Structured Streaming pipeline
│   ├── schema.py               # Spark DataFrame schema definition
│   └── streaming_job.py        # Stream processing, Pandas UDF model scoring, MongoDB sink
├── project_report.md           # Comprehensive technical project report
├── demo_commands.md            # Execution guide for multi-laptop setup
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

## ⚙️ Prerequisites & Setup

### 1. Software Requirements
- **Java**: OpenJDK 11 or 17
- **Python**: 3.10+
- **Apache Kafka & Zookeeper**: 3.x
- **Apache Spark**: 3.5.x / 4.x
- **MongoDB**: Local or Cloud instance (`mongodb://127.0.0.1:27017`)
- **Snowflake Account**: Data warehouse credentials

### 2. Environment Setup

```bash
# Clone repository
git clone https://github.com/Runa-Rameena/Credit-card-Anomaly-detection.git
cd Credit-card-Anomaly-detection

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy `.env.example` to `config/.env` and update with your credentials:

```bash
cp config/.env.example config/.env
```

---

## 🚀 Running the Pipeline

Follow the step-by-step execution flow:

### Step 1: Train the Anomaly Detection Model (Optional)
```bash
python -m anomaly_detection.train_model
```

### Step 2: Start Zookeeper & Kafka Broker
```bash
# Terminal 1: Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Kafka Broker
bin/kafka-server-stop.sh && bin/kafka-server-start.sh config/server.properties

# Terminal 3: Create Topic (Run once)
bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Step 3: Start Kafka Producer
```bash
python -m producer.kafka_producer
```

### Step 4: Run Spark Structured Streaming Job
```bash
export SPARK_MASTER_URL="spark://<YOUR_MASTER_IP>:7077"  # Or spark://localhost:7077
python -m spark_streaming.streaming_job
```

### Step 5: Launch Streamlit Dashboard
```bash
streamlit run dashboard/app.py
```
*Access the live dashboard at: `http://localhost:8501`*

---

## 📊 Dashboard & Monitoring

- **Live Metrics**: Total processed transactions, anomaly counts, fraud rate percentage, total transaction volume.
- **Snowflake Analytics**: Real-time record counts synchronized with Snowflake.
- **Interactive Tabs**:
  1. 📡 **Live Feed**: MongoDB real-time stream table.
  2. 🚨 **Anomalies**: Filtered list of flagged fraudulent transactions.
  3. 📊 **Charts**: Distribution graphs and category breakdown.
  4. ❄️ **Snowflake Query**: Execute direct SQL queries against Snowflake warehouse.
- **Manual Fraud Injector**: Single-click button to inject synthetic $45,000 darkweb transactions to test model detection latency live.

---

## 📄 Documentation

- [Detailed Technical Report](project_report.md)
- [Multi-Node Cluster Setup & Demo Commands Guide](demo_commands.md)

---

## 📜 License & Acknowledgments

Developed as part of the **Big Data Analytics (22AIE312)** curriculum at **Amrita Vishwa Vidyapeetham, Chennai**. Special thanks to **Dr. S. Saravanan** for guidance and mentorship throughout the project development.
