import argparse
import json
import logging
import signal
import sys
import time
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    BATCH_SIZE,
    BATCH_TIMEOUT_SECONDS,
    STORAGE_MODE,
    PROCESSED_DATA_DIR
)
from app.processing.validator import EventValidator
from app.processing.processor import EventProcessor
from app.storage.local_storage import LocalStorageWriter
from app.storage.s3_storage import S3StorageWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [CONSUMER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ClickstreamConsumer")


class ClickstreamConsumer:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_GROUP_ID,
        batch_size: int = BATCH_SIZE,
        batch_timeout_seconds: int = BATCH_TIMEOUT_SECONDS,
        storage_mode: str = STORAGE_MODE
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.storage_mode = storage_mode

        self.consumer: Optional[KafkaConsumer] = None
        self.running = False

        self.validator = EventValidator()
        self.processor = EventProcessor()
        self.local_writer = LocalStorageWriter()
        self.s3_writer = S3StorageWriter() if storage_mode == "s3" else None

        # Batch buffers & tracking
        self.valid_batch: List[Dict[str, Any]] = []
        self.rejected_batch: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.total_processed_count = 0
        self.total_rejected_count = 0
        self.total_batches_written = 0

    def connect(self, retries: int = 5, delay: float = 2.0) -> bool:
        """Establish connection to Kafka broker and join consumer group."""
        logger.info(f"Connecting Consumer (group='{self.group_id}') to Kafka at {self.bootstrap_servers}...")
        for attempt in range(1, retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,  # Manual commit after batch processing
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000  # Poll timeout 1 sec
                )
                logger.info(f"Successfully subscribed Consumer to topic '{self.topic}'.")
                return True
            except KafkaError as e:
                logger.warning(f"Consumer connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"Unexpected error connecting Consumer: {e}")
                if attempt < retries:
                    time.sleep(delay)

        logger.error(f"Consumer could not connect to Kafka at {self.bootstrap_servers} after {retries} attempts.")
        return False

    def flush_batch(self):
        """Write current valid batch buffer to Parquet storage and flush rejected items."""
        if not self.valid_batch and not self.rejected_batch:
            self.last_flush_time = time.time()
            return

        batch_count = len(self.valid_batch)
        rejected_count = len(self.rejected_batch)

        logger.info(f"[BATCH] Triggering batch write: {batch_count} valid events, {rejected_count} rejected events.")

        # Process valid batch
        if batch_count > 0:
            df = self.processor.events_to_dataframe(self.valid_batch)
            written_paths = self.local_writer.write_batch(df)
            self.total_batches_written += len(written_paths)
            self.total_processed_count += batch_count

            logger.info(f"[STORAGE] Wrote batch of {batch_count} events to {len(written_paths)} Parquet file(s).")

            # Upload to S3 if cloud mode enabled
            if self.storage_mode == "s3" and self.s3_writer:
                uploaded = self.s3_writer.upload_local_batch_paths(written_paths, PROCESSED_DATA_DIR)
                logger.info(f"[S3] Uploaded {len(uploaded)}/{len(written_paths)} Parquet file(s) to S3 bucket.")

            self.valid_batch.clear()

        # Handle rejected items
        if rejected_count > 0:
            rej_path = self.local_writer.write_rejected(self.rejected_batch)
            self.total_rejected_count += rejected_count
            logger.info(f"[STORAGE] Logged {rejected_count} invalid events to {rej_path}.")
            self.rejected_batch.clear()

        # Commit offset after successful batch storage
        if self.consumer:
            try:
                self.consumer.commit()
                logger.info("[KAFKA] Committed offset for processed batch.")
            except Exception as e:
                logger.error(f"[KAFKA] Error committing consumer offset: {e}")

        self.last_flush_time = time.time()

    def start(self, max_batches: Optional[int] = None):
        """Continuously poll messages from Kafka and manage batch pipeline."""
        if not self.consumer:
            if not self.connect():
                sys.exit(1)

        self.running = True
        logger.info(
            f"Consumer pipeline active. Batch size: {self.batch_size}, "
            f"Timeout: {self.batch_timeout_seconds}s, Mode: {self.storage_mode.upper()}"
        )

        batches_processed = 0

        try:
            while self.running:
                # Poll messages from Kafka broker
                try:
                    records = self.consumer.poll(timeout_ms=1000)
                except KafkaError as e:
                    logger.error(f"Kafka polling error: {e}")
                    time.sleep(1.0)
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error during poll: {e}")
                    time.sleep(1.0)
                    continue

                for partition, messages in records.items():
                    for msg in messages:
                        payload = msg.value
                        logger.info(f"Received message from partition={msg.partition} offset={msg.offset}")

                        # 1. Validate event
                        is_valid, rejection_reason = self.validator.validate(payload)
                        if not is_valid:
                            logger.warning(f"[VALIDATION] Rejected event: {rejection_reason}")
                            self.rejected_batch.append({
                                "payload": payload,
                                "rejection_reason": rejection_reason,
                                "rejected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                            })
                            continue

                        # 2. Process event (deduplication & UTC normalization)
                        is_proc_ok, processed_event, proc_err = self.processor.process_event(payload)
                        if not is_proc_ok:
                            logger.warning(f"[PROCESSOR] Deduplicated/Skipped: {proc_err}")
                            if "Duplicate" in str(proc_err):
                                # Skip duplicates without failing
                                continue
                            self.rejected_batch.append({
                                "payload": payload,
                                "rejection_reason": proc_err,
                                "rejected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                            })
                            continue

                        # 3. Append to valid batch buffer
                        self.valid_batch.append(processed_event)

                # Check batch size or timeout trigger
                time_since_flush = time.time() - self.last_flush_time
                if len(self.valid_batch) >= self.batch_size or time_since_flush >= self.batch_timeout_seconds:
                    if self.valid_batch or self.rejected_batch:
                        self.flush_batch()
                        batches_processed += 1
                        if max_batches is not None and batches_processed >= max_batches:
                            logger.info(f"Reached max batches target ({max_batches}). Stopping consumer.")
                            break

        except KeyboardInterrupt:
            logger.info("Consumer loop interrupted by user signal.")
        finally:
            self.stop()

    def stop(self):
        """Flush remaining items in buffer and close consumer gracefully."""
        self.running = False
        logger.info("Flushing final batch buffer...")
        self.flush_batch()
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            self.consumer = None
            logger.info("Kafka consumer cleanly closed.")


def main():
    parser = argparse.ArgumentParser(description="Real-Time Clickstream Kafka Consumer")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Batch size trigger")
    parser.add_argument("--batch-timeout", type=int, default=BATCH_TIMEOUT_SECONDS, help="Batch timeout seconds")
    parser.add_argument("--storage-mode", type=str, default=STORAGE_MODE, choices=["local", "s3"], help="Storage target mode")
    parser.add_argument("--max-batches", type=int, default=None, help="Stop consumer after N batches")
    args = parser.parse_args()

    consumer = ClickstreamConsumer(
        batch_size=args.batch_size,
        batch_timeout_seconds=args.batch_timeout,
        storage_mode=args.storage_mode
    )

    # Attach signal handlers for graceful stop
    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received. Shutting down consumer...")
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer.start(max_batches=args.max_batches)


if __name__ == "__main__":
    main()
