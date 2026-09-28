# Real-Time Clickstream Data Ingestion Pipeline using Kafka and Cloud Storage

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![Kafka 3.7.0](https://img.shields.io/badge/kafka-3.7.0-black.svg)](https://kafka.apache.org/)
[![Streamlit](https://img.shields.io/badge/streamlit-1.30+-red.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A complete, professional, production-inspired real-time clickstream data ingestion pipeline and analytical dashboard built with Apache Kafka, Python, Parquet batching, AWS S3, and Streamlit.

---

## 📌 Project Overview

E-commerce platforms generate continuous high-velocity user activity events (page views, product clicks, add-to-cart, purchases). Storing every single event directly to traditional databases causes performance bottlenecks and scale failures.

This project implements a scalable real-time streaming pipeline:
1. Simulates realistic user clickstream events.
2. Streams events to an **Apache Kafka** broker configured in KRaft mode.
3. Consumes, validates, and deduplicates event streams in real time.
4. Aggregates events into **micro-batches** (100 events or 10-second timeout).
5. Writes columnar **Parquet files** partitioned by date (`year=YYYY/month=MM/day=DD/`).
6. Optionally uploads batches to **AWS S3** cloud storage.
7. Visualizes conversion funnels and user metrics on a near real-time **Streamlit** dashboard.

---

## 🏗️ Architecture Diagram

```text
                  LOCAL MACHINE / DOCKER COMPOSE

        ┌─────────────────────────┐
        │  Clickstream Generator  │  python -m app.producer
        │ (app/processing/gen.py) │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │     Kafka Producer      │  JSON Serializer
        │  (app/producer/prod.py) │  Partition Key: user_id
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │   Apache Kafka Broker   │  KRaft Mode (apache/kafka:3.7.0)
        │    Topic: clickstream   │  3 Partitions
        │     (localhost:9092)    │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │     Kafka Consumer      │  python -m app.consumer
        │  (app/consumer/cons.py) │  Group: clickstream-consumer
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │ Validation & Processing │  - Schema Validator & UTC Normalizer
        │ (app/processing/...)    │  - Deduplicator (event_id)
        └────────────┬────────────┘  - Invalid -> data/rejected/
                     │
                     ▼
        ┌─────────────────────────┐
        │    Batch Data Writer    │  100 events OR 10s timeout
        └──────┬───────────┬──────┘
               │           │
               ▼           ▼
   ┌───────────────┐   ┌────────────────┐
   │ Local Parquet │   │ AWS S3 Cloud   │
   │ data/processed│   │ (if enabled)   │
   └───────┬───────┘   └────────────────┘
           │
           ▼
   ┌──────────────────────────────────┐
   │ Near Real-Time Streamlit Dash    │  streamlit run dashboard/app.py
   │ (dashboard/app.py)               │
   └──────────────────────────────────┘
```

---

## 🛠️ Technology Stack

- **Streaming Backbone**: Apache Kafka 3.7.0 (KRaft mode), `kafka-python-ng`
- **Data Processing**: Python 3.10+, Pandas, PyArrow
- **Cloud Storage**: AWS S3, `boto3`
- **Dashboard & Visualization**: Streamlit, Plotly Express
- **Containerization & Orchestration**: Docker, Docker Compose
- **Testing & Environment**: `pytest`, `moto`, `python-dotenv`

---

## 🔄 Data Flow Lifecycle

```text
Event Generator → Producer → Kafka Topic → Consumer → Validation → Processing → Batch Storage → S3 Upload → Analytics → Dashboard
```

1. **Event Generation**: Realistic e-commerce user actions (`page_view`, `product_view`, `search`, `add_to_cart`, `remove_from_cart`, `wishlist`, `checkout`, `purchase`, `logout`) generated with UTC ISO timestamps.
2. **Kafka Streaming**: Producer publishes JSON payload to Kafka topic `clickstream` using `user_id` as partition key.
3. **Consumer Polling**: Consumer group `clickstream-consumer` polls messages across 3 partitions.
4. **Validation & Processing**: Schema validator checks required fields; processor normalizes timestamps to UTC, computes `ingestion_timestamp`, and deduplicates `event_id`s.
5. **Batch Storage**: Records are buffered into micro-batches (100 records or 10s timeout) and written to partitioned Parquet files (`data/processed/year=YYYY/month=MM/day=DD/`).
6. **S3 Upload**: When `STORAGE_MODE=s3`, batches automatically upload to AWS S3.
7. **Near Real-Time Dashboard**: Streamlit reads Parquet files and displays conversion rates, timelines, and demographic breakdowns.

---

## 📖 Key Kafka Concepts Explained

- **Producer**: Application component that sends event messages to Kafka topics.
- **Broker**: A Kafka server instance that receives, stores, and serves message streams.
- **Topic**: A logical log category or stream name where messages are published (e.g., `clickstream`).
- **Partition**: A topic is split into multiple parallel log partitions distributed across brokers for parallelism and scale.
- **Consumer**: Application component that reads message streams from Kafka topics.
- **Consumer Group**: A group of consumers working together to read from a topic, where each partition is assigned to a single consumer instance within the group.
- **Offset**: A unique sequential integer assigned to each message within a partition, marking consumer progress.

---

## ⚡ Quick Start & Execution Commands

### 1. Environment Setup

Clone the repository and set up a Python virtual environment:

```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

Create `.env` file:
```bash
cp .env.example .env
```

### 2. Preview Dashboard with Sample Data (No Docker required)

Generate 10,000 clean sample clickstream events (cleans previous sample files by default):

```bash
python scripts/generate_sample_data.py --count 10000
streamlit run dashboard/app.py
```

If you want to append additional sample data to existing files without overwriting:
```bash
python scripts/generate_sample_data.py --count 5000 --append
```
Open your browser at `http://localhost:8501`. The dashboard displays **Demo / Sample Data Mode**.

### 3. Run Live Kafka Pipeline with Docker

Start the Apache Kafka KRaft broker:

```bash
docker compose up -d
```

Verify Kafka container status and topic partitions:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Run Kafka Consumer (Terminal 1):
```bash
python -m app.consumer
```

Run Kafka Producer (Terminal 2):
```bash
python -m app.producer --count 250 --rate 5
```

Launch Dashboard (Terminal 3):
```bash
streamlit run dashboard/app.py
```
The dashboard automatically switches to **Near Real-Time Live Pipeline** mode reading live Parquet files!

---

## ☁️ AWS S3 Cloud Setup (Optional Phase 2)

To enable AWS S3 cloud uploads:

1. Create an S3 bucket in your AWS account (e.g., `my-clickstream-bucket`).
2. Set environment variables in `.env`:
   ```env
   STORAGE_MODE=s3
   AWS_REGION=us-east-1
   AWS_S3_BUCKET=my-clickstream-bucket
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   ```
3. Restart the consumer (`python -m app.consumer`). Processed Parquet files will be stored locally AND uploaded to `s3://my-clickstream-bucket/processed/year=...`.

---

## 🧪 Testing

Run the full automated test suite with `pytest`:

```bash
pytest -v
```

All 19 unit tests verify generator logic, validator rules, deduplication, batch storage, S3 mock upload (`moto`), and conversion formulas.

---

## 🚀 Future Improvements (Roadmap)

- **Stream Processing Engine**: Integrate Apache Flink or Apache Spark Streaming for sliding-window analytics.
- **Schema Management**: Implement Confluent Schema Registry with Avro format serialization.
- **Cloud Query Engine**: Configure AWS Glue Crawler and AWS Athena for ad-hoc SQL querying over S3 Parquet partitions.
- **Dead Letter Queue (DLQ)**: Route rejected invalid events to a secondary Kafka DLQ topic.
- **ML Behavior Prediction**: Add real-time user purchase intent prediction models.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
