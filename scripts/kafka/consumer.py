"""
Kafka Consumer - US Flight Delays Pipeline
Reads messages from Kafka topic 'flights-raw' and inserts them into bronze.flights.
Runs continuously until interrupted (Ctrl+C) or max_messages limit is reached.

Usage:
    python consumer.py                        # runs until Ctrl+C
    python consumer.py --max-messages 100000  # stops after N messages
"""

import json
import logging
import argparse
import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC             = "flights-raw"
KAFKA_GROUP_ID          = "flights-bronze-consumer"
KAFKA_AUTO_OFFSET_RESET = "earliest"

PG_CONN = {
    "host":     "localhost",
    "port":     5432,
    "database": "flights_db",
    "user":     "admin",
    "password": "admin123",
}

BATCH_SIZE         = 1000
BATCH_LOG_INTERVAL = 10000

COLUMNS = [
    "FL_DATE", "AIRLINE", "AIRLINE_DOT", "AIRLINE_CODE", "DOT_CODE",
    "FL_NUMBER", "ORIGIN", "ORIGIN_CITY", "DEST", "DEST_CITY",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "TAXI_OUT", "WHEELS_OFF",
    "WHEELS_ON", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY",
    "CANCELLED", "CANCELLATION_CODE", "DIVERTED", "CRS_ELAPSED_TIME",
    "ELAPSED_TIME", "AIR_TIME", "DISTANCE", "DELAY_DUE_CARRIER",
    "DELAY_DUE_WEATHER", "DELAY_DUE_NAS", "DELAY_DUE_SECURITY",
    "DELAY_DUE_LATE_AIRCRAFT"
]


def get_pg_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(**PG_CONN)


def ensure_bronze_table(conn) -> None:
    """Create bronze schema and flights table if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze.flights (
                {', '.join(f'{col} TEXT' for col in COLUMNS)}
            );
        """)
    conn.commit()
    log.info("✅ bronze.flights table ready")


def consume_and_insert(max_messages: int = None) -> None:
    """Consume messages from Kafka and insert into bronze.flights in batches."""
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} …")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=BATCH_SIZE,
    )

    conn = get_pg_connection()
    ensure_bronze_table(conn)

    insert_sql = f"""
        INSERT INTO bronze.flights ({', '.join(f'"{col}"' for col in COLUMNS)})
        VALUES ({', '.join(['%s'] * len(COLUMNS))})
    """

    batch = []
    total = 0

    log.info("Starting consumption … Press Ctrl+C to stop.")
    try:
        for msg in consumer:
            row = msg.value
            values = tuple(row.get(col, None) for col in COLUMNS)
            batch.append(values)

            if len(batch) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    execute_batch(cur, insert_sql, batch)
                conn.commit()
                total += len(batch)
                batch = []

                if total % BATCH_LOG_INTERVAL == 0:
                    log.info(f"  Inserted {total:,} rows into bronze.flights …")

            if max_messages and total >= max_messages:
                log.info(f"Reached max_messages limit: {max_messages:,}")
                break

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        if batch:
            with conn.cursor() as cur:
                execute_batch(cur, insert_sql, batch)
            conn.commit()
            total += len(batch)

        consumer.close()
        conn.close()
        log.info(f"✅ Consumer done — total inserted: {total:,} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer for flight data")
    parser.add_argument("--max-messages", type=int, default=None,
                        help="Stop after N messages")
    args = parser.parse_args()
    consume_and_insert(max_messages=args.max_messages)