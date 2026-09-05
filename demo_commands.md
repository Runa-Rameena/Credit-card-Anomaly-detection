# Big Data Pipeline: Demo Guide

This document contains everything you need to start the pipeline and configure it for a multi-laptop cluster.

## 🔗 Quick Links (Replace `<MASTER_IP>` with today's IP, e.g., `11.12.3.192`)

- **Hadoop (HDFS):** `http://<MASTER_IP>:9870`
- **YARN:** `http://<MASTER_IP>:8088`
- **Spark Master:** `http://<MASTER_IP>:8080`
- **Spark Worker:** `http://<MASTER_IP>:8081`
- **Dashboard:** `http://<MASTER_IP>:8501`

### 🔎 How to Find Your IP Address Today (Do this first!)
Because your IP changes daily on WiFi, run this command on your **Main Laptop (Laptop 1)** to find it:
```bash
hostname -I
```
*(Look for the first number it outputs, which usually looks like `192.168.x.x` or `11.x.x.x`)*
---

## 💻 Laptop 1 (Master): Your Main Laptop

This laptop will run all the core services (Hadoop, Kafka, Spark Master) AND the producer/dashboard.

### Step 1: Set Today's IP Address & PySpark Environment
Run this in **every new terminal** you open on Laptop 1:
```bash
# Replace 11.12.3.192 with whatever your IP is today
export MASTER_IP=11.12.3.192

# Ensure your Python environment matches the Spark runtime (4.1.0)
cd ~/projects/credit_card_anomaly
source venv/bin/activate
pip install pyspark==4.1.0
```

### Step 2: Start Core Infrastructure Services
Open a terminal, set `$MASTER_IP`, and run:
```bash
# 1. Format and start Hadoop/YARN
hdfs namenode -format
start-dfs.sh
start-yarn.sh

# 2. Start Spark Master bound to your dynamic IP
SPARK_MASTER_HOST=$MASTER_IP $SPARK_HOME/sbin/start-master.sh

# 3. Start a Spark Worker on your laptop too
$SPARK_HOME/sbin/start-worker.sh spark://$MASTER_IP:7077
```

### Step 3: Run the Pipeline Terminals
Open these as separate terminals. Remember to export `$MASTER_IP` if needed!

```bash
# ── Terminal 1 — Zookeeper ──────────────────────────────────
cd ~/bigdata/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

```bash
# ── Terminal 2 — Kafka ──────────────────────────────────────
cd ~/bigdata/kafka
bin/kafka-server-start.sh config/server.properties
```

```bash
# ── Terminal 3 — Create Topic (run once, then close) ────────
~/bigdata/kafka/bin/kafka-topics.sh --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

```bash
# ── Terminal 4 — Producer ───────────────────────────────────
cd ~/projects/credit_card_anomaly
source venv/bin/activate
python -m producer.kafka_producer
```

```bash
# ── Terminal 5 — Spark Streaming ────────────────────────────
cd ~/projects/credit_card_anomaly
source venv/bin/activate

# Pass today's IP so the code knows where the master is
export SPARK_MASTER_URL="spark://$MASTER_IP:7077"
python -m spark_streaming.streaming_job
```

```bash
# ── Terminal 6 — Dashboard ──────────────────────────────────
cd ~/projects/credit_card_anomaly
source venv/bin/activate
streamlit run dashboard/app.py
# View at http://$MASTER_IP:8501
```

---

## 💻 Laptop 2 (Worker): The Second Laptop

This laptop ONLY needs to run the Spark Worker to connect to your Master and help process data. Username differences do not matter here since we run it manually.

### Step 1: Get Master's IP
Ask the person on Laptop 1 for their IP address (e.g., `11.12.3.192`).
Run this in the terminal:
```bash
export MASTER_IP=11.12.3.192
```

### Step 2: Align PySpark & Start the Worker
Run the following commands to ensure your PySpark versions perfectly match Laptop 1 and connect the worker:
```bash
# 1. Activate your python environment
cd ~/projects/credit_card_anomaly
source venv/bin/activate

# 2. MATCH your PySpark version with Laptop 1 (CRITICAL)
pip install pyspark==4.1.0

# 3. Ensure Spark is installed. Provide exact path to spark folder.
$SPARK_HOME/sbin/start-worker.sh spark://$MASTER_IP:7077
```

### Step 3: Confirm the Connection!
To verify that everything worked and Laptop 2 is successfully connected:
1. Open your browser and go to your Spark Master UI: `http://<MASTER_IP>:8080` (Replace `<MASTER_IP>` with Laptop 1's actual IP).
2. Look under the **"Workers"** table on the page. 
3. You should see a new row listed with the IP of Laptop 2, and it should say **"ALIVE"** under the state!

---

## 🛑 Stopping the Services (Cleanup)

When you are done with the demo, you can gracefully stop all the background Java (`jps`) processes by running these commands on **Laptop 1**:

```bash
# 1. Stop Kafka & Zookeeper
cd ~/bigdata/kafka
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh

# 2. Stop Spark Master & Worker
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh

# 3. Stop Hadoop & YARN
stop-yarn.sh
stop-dfs.sh
```

*(You can also simply interrupt (`Ctrl+C`) the Streaming Job, Producer, and Dashboard in their respective terminals to close the Python processes.)*
