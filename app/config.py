import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

# Application Configuration
APP_ENV = os.getenv("APP_ENV", "local")

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickstream-consumer")

# Event Generator Settings
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))

# Batch Processing Settings
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT_SECONDS = int(os.getenv("BATCH_TIMEOUT_SECONDS", "10"))

# Storage Settings
STORAGE_MODE = os.getenv("STORAGE_MODE", "local").lower()

# AWS S3 Settings
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "clickstream-data-bucket")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# Directory Paths
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REJECTED_DATA_DIR = DATA_DIR / "rejected"
SAMPLE_DATA_DIR = DATA_DIR / "sample"

# Ensure essential directories exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, REJECTED_DATA_DIR, SAMPLE_DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)
