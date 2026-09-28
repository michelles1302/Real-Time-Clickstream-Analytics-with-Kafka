import argparse
import json
import logging
import sys
import time
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from app.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, EVENTS_PER_SECOND
from app.processing.event_generator import ClickstreamEventGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [PRODUCER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("ClickstreamProducer")

class ClickstreamProducer:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: Optional[KafkaProducer] = None
        self.generator = ClickstreamEventGenerator()

    def connect(self, retries: int = 5, delay: float = 2.0) -> bool:
        """Establish connection to Kafka broker with retry logic."""
        logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers}...")
        for attempt in range(1, retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",  # Ensure strong durability
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info(f"Successfully connected to Kafka broker on attempt {attempt}.")
                return True
            except KafkaError as e:
                logger.warning(f"Connection attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    time.sleep(delay)
            except Exception as e:
                logger.error(f"Unexpected error initializing Kafka producer: {e}")
                if attempt < retries:
                    time.sleep(delay)

        logger.error(f"Could not connect to Kafka broker at {self.bootstrap_servers} after {retries} attempts.")
        return False

    def send_event(self, event: dict) -> bool:
        """Send a single event to the Kafka topic."""
        if not self.producer:
            logger.error("Producer is not connected to Kafka.")
            return False

        event_id = event.get("event_id", "unknown")
        user_id = event.get("user_id", "")

        try:
            # Use user_id as key for consistent partition routing
            future = self.producer.send(self.topic, key=user_id, value=event)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Sent event {event_id} -> topic={record_metadata.topic} "
                f"partition={record_metadata.partition} offset={record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Kafka error delivering event {event_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to send event {event_id}: {e}")
            return False

    def produce(
        self,
        count: Optional[int] = None,
        events_per_sec: float = EVENTS_PER_SECOND,
        invalid_ratio: float = 0.05
    ):
        """Produce clickstream events continuously or up to count limit."""
        if not self.producer:
            if not self.connect():
                sys.exit(1)

        logger.info(
            f"Starting event production to topic '{self.topic}' "
            f"({events_per_sec} events/sec, count={count if count else 'infinite'})..."
        )

        sent_count = 0
        interval = 1.0 / max(events_per_sec, 0.1)

        try:
            while True:
                if count is not None and sent_count >= count:
                    logger.info(f"Reached targeted produce limit of {count} events. Stopping.")
                    break

                # Periodically generate an invalid event to test validator pipeline
                force_invalid = (invalid_ratio > 0 and (sent_count + 1) % int(1.0 / invalid_ratio) == 0)
                event = self.generator.generate_event(force_invalid=force_invalid)

                if self.send_event(event):
                    sent_count += 1

                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user.")
        finally:
            self.close()

    def close(self):
        """Flush and close producer connection gracefully."""
        if self.producer:
            logger.info("Flushing pending events and closing Kafka producer...")
            self.producer.flush(timeout=5)
            self.producer.close(timeout=5)
            logger.info("Kafka producer closed.")
            self.producer = None


def main():
    parser = argparse.ArgumentParser(description="Real-Time Clickstream Kafka Producer")
    parser.add_argument("--count", type=int, default=None, help="Number of events to produce (default: infinite)")
    parser.add_argument("--rate", type=float, default=EVENTS_PER_SECOND, help="Events per second rate")
    parser.add_argument("--bootstrap-servers", type=str, default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC, help="Kafka target topic")

    args = parser.parse_args()

    producer = ClickstreamProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
    producer.produce(count=args.count, events_per_sec=args.rate)


if __name__ == "__main__":
    main()
