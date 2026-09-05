# Credit Card Anomaly Detection Pipeline Report

## Background
The rapid digitization of global commerce has brought about a significant rise in electronic payment mechanisms, particularly credit cards. This surge comes with an escalated risk of sophisticated financial fraud. Financial institutions are under constant pressure to identify and block fraudulent transactions within milliseconds of a swipe. Historically, banks have employed large-scale rule-based engines that flag transactions based on hard-coded heuristics (e.g., *“if transaction > $5,000 in a foreign country, block it”*). This mechanism is highly flawed and deterministic, failing when modern fraudsters adapt their spatial and temporal patterns to avoid standard thresholds. 

To counter this, the industry is shifting towards behavioral Machine Learning at the network edge. Unlike classical batch-processing where models run overnight, modern architectures score the transaction in real-time immediately after it hits the bank ledger. This pipeline represents a scalable big data architecture capable of detecting anomalies in real-time using continuous event-driven data streams.

## Problem Statement
Developing a sub-second latency fraud detection pipeline introduces significant engineering complexities. Modern banking networks experience thousands of transactions per second (TPS). The primary challenge is constructing a robust infrastructure that can ingest these continuous massive data streams without bottlenecking, apply complex machine learning models efficiently, and flag anomalies before transactions are finalized.

Furthermore, physical data privacy introduces another problem: using real Personal Identifiable Information (PII) for development violates PCI-DSS, GDPR, and CCPA constraints. A system must be developed and stress-tested securely without relying on strict, unencrypted, and static datasets that fail to mimic real-world velocity.

## Objective
The core objective of this project is to implement an end-to-end, multi-node big data pipeline for real-time credit card anomaly detection. Utilizing Apache Spark Structured Streaming, Apache Kafka, and Unsupervised Machine Learning, the system is designed to:
1. Ingest high-throughput, real-time transaction streaming natively via an immutable event bus.
2. Digest incoming byte data using completely fault-tolerant micro-batch processing.
3. Rapidly synthesize fraud ratings using PySpark distributed workers without aggregating metrics back to the driver.
4. Provide immediate access to inferred fraud analytics via low-latency operational databases (MongoDB) and historical warehousing (Snowflake).
5. Allow scalable horizontal computing by attaching multiple compute laptops ("Workers") over a single dynamic Wi-Fi network.

## Dataset Description
A key characteristic of this project is the use of a synthetically generated schema (`producer.transaction_generator`) designed to perfectly mimic real-world transaction patterns. 
- **Training Size:** 10,000 historical transactions heavily engineered to enforce uniform "normal" patterns.
- **Why Synthetic Data is OK for this pipeline:** 
  1. *Regulatory Control:* It successfully bypasses aggressive personal data privacy constraints (GDPR/PCI-DSS) by guaranteeing zero authentic PII exposure.
  2. *Volume Control:* Static CSV databases downloaded from public sources are bounded. A synthetic generator acts as a controllable fire-hose; developers can intentionally flood Kafka with 10,000 TPS to observe JVM heap degradation, latency spikes, and tuning limitations on the infrastructure.
  3. *Ground Truth Validation:* By generating transactions programmatically, the anomaly proportions are mathematically known. If an anomaly is injected, the model's precise recall can be benchmarked immediately. 

**Key Features Extracted within the Dataset:**
- `amount`: The transaction monetary value.
- `hour`: The hour precisely extracted from the ISO-8601 string timestamp.
- `is_online_num`: Integer casting of the boolean flag representing an online medium.
- `latitude` & `longitude`: Geolocation variables corresponding to the transaction origin.

## Methodology / Models Implemented
The pipeline infrastructure relies on converging specific, tightly integrated Data Engineering and software components. Every layer exists to solve a very specific big data constraint:

**1. The Producer Application (`kafka_producer.py`)**
* **Why it exists:** Real-world transactions do not arrive in neatly packaged CSV files; they arrive dynamically as JSON webhooks at all hours of the day. Our local Spark cluster needs a simulator to mimic this unpredictability.
* **How it works:** This python script runs in a continuous "While True" loop, repeatedly instantiating fraudulent and regular JSON payloads. The script acts as the very entry-point of our architecture, beaming bytes dynamically to our Kafka ingestion ports.

**2. Streaming Ingestion: Apache Kafka & Zookeeper**
* **Why it exists:** If Spark was directly connected to the Producer and Spark crashed, every transaction during that downtime would be lost forever. Kafka acts as an immutable distributed commit log (a "shock absorber"). 
* **How it works:** Zookeeper acts as the cluster manager, determining which Kafka node controls our `transactions` topic. Kafka receives the raw JSON bytes from the producer and holds them in memory queues on the hard drive. Kafka doesn't compute data; it guarantees flawless message passing. When Spark recovers from a crash, it asks Kafka exactly where it left off (via offsets), and Kafka delivers the missing payloads flawlessly.

**3. Compute Engine: Apache Spark Structured Streaming**
* **Why it exists:** Python natively is highly restricted by the Global Interpreter Lock (GIL) and is terrible at processing 10,000 JSONs per second. Apache Spark is a distributed computing JVM engine explicitly built for tearing apart Big Data.
* **How it works:** Spark establishes a primary "Driver" daemon on Laptop 1. The code instructs Spark to subscribe to the Kafka topic and pull data in 15-second "micro-batches". It then converts the underlying raw JSON streams into structured DataFrames (tables) so they can be filtered, typed, and mutated simultaneously across all connected Worker node laptops around the network.

**4. Distributed PySpark Logic (Apache Arrow)**
* **Why it exists:** Typically, if a user wants to run a Python algorithm (like Isolation Forest), Spark has to drag all the worker's data *back* to Laptop 1, convert it from Java memory to Python, score it, and send it back. This massive serialization bottleneck defeats the entire purpose of having dozens of computers helping!
* **How it works:** The architecture fundamentally bypasses this by implementing a PySpark **Pandas User-Defined Function (UDF)** augmented mathematically by Apache Arrow. Arrow standardizes how bytes look in memory between Java and Python. This allows Spark to instantly scatter partitioned Kafka micro-batches to the background workers (e.g. Laptop 2), load the pre-trained ML `.pkl` cache into its local RAM, and run high-efficiency pandas operations *where the data physically sits*, achieving native speedups and massive network relief. 

**5. Target Storage Sinks: MongoDB & Snowflake**
* **Why they exist:** A calculated fraud score is useless if it isn't routed to systems that can act on it. Furthermore, different applications require totally different database formats.
* **How it works:** Spark's `foreachBatch` writer is explicitly configured for dual-commit sinks. It writes the clean, post-processed Spark schema directly into **MongoDB** (a NoSQL document store). MongoDB natively thrives at lightning-fast `GET` queries, perfectly supporting our live-polling Streamlit front-end Dashboard. Immediately after, it pushes identical records into **Snowflake** (a highly rigid Data Warehouse), creating the perfect historical staging table for internal data scientists to perform retroactive anomaly analytics on billion-row scales. 

## Model Selection and Training
**Model Selected**: `sklearn.ensemble.IsolationForest`

**Rationale for Selection:**
Fraud labels are implicitly unavailable in real-time. Therefore, supervised algorithms (Random Forests, Neural Networks) are difficult to implement without massive pre-tagged datasets. The Isolation Forest operates as an Unsupervised Ensemble algorithm. It works on the mathematical premise that anomalies are "few and different". It randomly selects a feature and splits it between minimum and maximum bounds. Since out-of-scale fraudulent anomalies sit on the far mathematical edge of the distributions, they naturally require significantly fewer logical splits to isolate than clustered, "normal" dense purchases. This makes the model incredibly fast, low-latency, and memory-efficient for real-time edge streaming.

**Training Specifications & Hyperparameters:**
- **Preprocessing:** Numerical vectors are flattened and scaled utilizing `StandardScaler` to ensure uniform mathematical impact during tree splitting.
- **Hyperparameters Enforced:** The tree builds off `n_estimators=100` subsets, relying on a `contamination=0.05` limit, under the assumption that 5% of all streaming global data equates to a legitimate anomaly.
- **Serialization Framework:** The pre-trained model and the exact scaler variance distributions are natively serialized via `joblib`. The Spark driver inherently broadcasts these `.pkl` binaries over the local network to all attached Workers prior to batch execution. 
- During `predict_anomaly()`, the worker model computes a `decision_function`, strictly flagging any resulting negative bounds (< 0) as `rule_anomaly = true`.

## Conclusion
This real-time Credit Card Anomaly Detection system successfully demonstrates that enterprise-grade fraud classification is inherently possible using consumer hardware. By integrating resilient Kafka brokers, Apache Spark Structured Streaming, distributed Pandas UDF inferencing, and dynamic NoSQL sinks, the pipeline presents a highly responsive edge-computing framework. The deliberate application of randomized synthetic datasets bypasses aggressive regulatory controls, simultaneously providing a controllable environment optimized for stress-testing multi-node cluster configurations. Ultimately, this architecture bridges infrastructural big-data throughput with mathematically localized Isolation Forest predictions, effectively meeting the sub-second latency constraints required for modern credit card protection.
