# System Architecture Documentation

## Real-Time Clickstream Data Ingestion Pipeline

### 1. Overall System Architecture

The system is a production-inspired real-time data ingestion pipeline that simulates e-commerce user interactions, streams events through Apache Kafka, processes and batches records into columnar Parquet format, and visualizes analytical insights using Streamlit.

```text
                  LOCAL MACHINE / DOCKER NETWORK

        ┌─────────────────────────┐
        │  Clickstream Generator  │  Generates realistic e-commerce events
        │ (app/processing/gen.py) │  (page_view, product_view, add_to_cart...)
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │     Kafka Producer      │  Serializes events to JSON, routes to topic
        │  (app/producer/prod.py) │  using user_id partition key
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │   Apache Kafka Broker   │  KRaft Mode (apache/kafka:3.7.0)
        │    Topic: clickstream   │  3 Partitions
        │  (localhost:9092 host)  │
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │     Kafka Consumer      │  Consumes messages in consumer group
        │  (app/consumer/cons.py) │  'clickstream-consumer'
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │ Validation & Processing │  - Strict JSON schema validator
        │ (app/processing/...)    │  - UTC timestamp normalizer & deduplicator
        └────────────┬────────────┘  - Rejected items routed to data/rejected/
                     │
                     ▼
        ┌─────────────────────────┐
        │    Batch Data Writer    │  Flushes every 100 events OR 10 seconds
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
   │ Near Real-Time Streamlit Dash    │
   │ (dashboard/app.py)               │
   └──────────────────────────────────┘
```

---

### 2. Deep-Dive Components

#### A. Event Generator & Producer
- **Generator**: Produces continuous or batch clickstream events with unique IDs (`evt_<seq>_<uuid>`), user IDs, session IDs, event types, UTC timestamps, device types, browsers, countries, and relevant product/page metadata.
- **Producer**: Uses `kafka-python-ng` to stream JSON events to the `clickstream` topic. Key partitioning by `user_id` ensures that all actions from the same user land in the same Kafka partition, preserving per-user temporal sequence.

#### B. Apache Kafka Broker (KRaft Mode)
- Running via Docker Compose using official `apache/kafka:3.7.0`.
- **KRaft Mode**: Replaces legacy ZooKeeper quorum with internal Raft consensus for metadata management, reducing latency and operational complexity.
- **Topic Configuration**: `clickstream` with **3 Partitions** to demonstrate distributed consumer group scaling and parallel stream reading.
- **Host Networking**: Exposed on port `9092` with `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092`.

#### C. Kafka Consumer & Processing Engine
- **Consumer Group**: `clickstream-consumer`.
- **Validation**: Strict schema checks (field presence, data types, allowed event types, ISO timestamp formatting, required `product_id` for product events). Invalid payloads are isolated in `data/rejected/rejected_<ts>.json`.
- **Processing**: Normalizes timestamps to UTC ISO strings, adds `ingestion_timestamp`, performs in-memory duplicate detection based on `event_id`, and adds date partition fields (`year`, `month`, `day`).

#### D. Batch Storage Strategy (Parquet + S3)
- **Batching Design**: Accumulates valid events into a memory buffer. Triggers batch writes when:
  - `len(batch) >= BATCH_SIZE` (default 100 events), OR
  - `time_since_last_flush >= BATCH_TIMEOUT_SECONDS` (default 10 seconds).
- **Parquet Storage**: Converts batches into Pandas DataFrames and writes them to local disk partitioned by date:
  ```text
  data/processed/year=YYYY/month=MM/day=DD/events_<timestamp>_<uuid>.parquet
  ```
- **Cloud Storage**: When `STORAGE_MODE=s3` is active, batch files are uploaded to AWS S3 using `boto3`:
  ```text
  s3://bucket/processed/year=YYYY/month=MM/day=DD/events_<timestamp>_<uuid>.parquet
  ```

#### E. Analytics & Streamlit Dashboard
- Calculates key metrics: Total Events, Unique Users, Unique Sessions, Total Purchases, Add-to-Cart Conversion Rate, Purchase Conversion Rate.
- Rendered via Plotly graphs with dynamic filters (date, event type, device, country).
- **Data Fallback**: Automatically checks `data/processed/` for live events, falling back to pre-generated sample data in `data/sample/` with a clear status header indicator (`Near Real-Time Live Pipeline` vs `Demo / Sample Data Mode`).

---

### 3. Core Architectural Questions & Decisions

#### Why Apache Kafka?
Kafka provides high-throughput, low-latency, fault-tolerant event streaming. It decouples event producers from consumers, acts as a durable log buffer during traffic spikes, and enables horizontal scaling via consumer group partition assignment.

#### Why Parquet instead of individual JSON files?
Writing individual JSON files per event creates a "small file problem" (high I/O metadata overhead, poor read performance). Parquet is a compressed, columnar file format optimized for analytical queries. It enables column pruning, efficient compression ratios (snappy/gzip), and fast scan speeds for analytical workloads.

#### Why S3 for Cloud Storage?
AWS S3 is the industry-standard cloud object store providing 99.999999999% (11 9s) durability, virtually infinite storage scalability, cost-effective tiered storage, and seamless integration with analytical query engines like AWS Athena, Spark, and Snowflake.

#### Why Kafka is NOT a Permanent Database
Kafka is designed as an append-only commit log for streaming event transport with time-based retention (e.g., 7 days). It is not optimized for ad-hoc SQL aggregation, complex joins, or historical analytics across months of data. Persistent columnar lakes (Parquet on S3/HDFS) are required for analytical queries.

---

### 4. Security & Error Handling

- **Secrets Management**: Credentials managed via environment variables (`.env`). `.env` and sensitive logs are excluded from version control via `.gitignore`.
- **Fault Tolerance**:
  - Malformed events route to `data/rejected/` without crashing consumer loop.
  - S3 uploads handle missing/invalid credentials gracefully, falling back to local Parquet storage without terminating the stream.
  - Producer incorporates retries and backoff for broker reconnections.
