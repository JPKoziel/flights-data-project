"""
Kafka Producer - US Flight Delays Pipeline
Reads CSV file and sends each row as a message to Kafka topic 'flights-raw'.
This enables queue-based ingestion instead of direct batch loading.

Usage:
    python producer.py                    # sends all rows
    python producer.py --limit 10000      # sends first N rows
"""

import csv
import json
import argparse
import logging
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "flights-raw"
RAW_CSV = os.path.join(os.path.dirname(__file__), "../../data/raw/flights_sample_3m.csv")
BATCH_LOG_INTERVAL      = 50000


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        batch_size=65536,
        linger_ms=10,
        compression_type="gzip",
    )


def send_flights_to_kafka(limit: int = None) -> None:
    """Read CSV and send each flight record as a Kafka message."""
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} …")
    producer = create_producer()

    log.info(f"Reading CSV from {RAW_CSV} …")
    sent = 0
    errors = 0

    with open(RAW_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if limit and sent >= limit:
                break
            try:
                producer.send(KAFKA_TOPIC, value=dict(row))
                sent += 1
                if sent % BATCH_LOG_INTERVAL == 0:
                    log.info(f"  Sent {sent:,} messages …")
            except KafkaError as e:
                log.error(f"Failed to send row {sent}: {e}")
                errors += 1

    producer.flush()
    producer.close()
    log.info(f"✅ Producer done — sent: {sent:,} messages, errors: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer for flight data")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to send")
    args = parser.parse_args()
    send_flights_to_kafka(limit=args.limit)