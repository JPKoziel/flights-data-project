"""
US Flight Delays - PySpark Pipeline
Processes data through Bronze -> Silver -> Gold layers.
Supports both full batch load and incremental (idempotent) mode.

Usage:
    python pipeline.py              # runs full pipeline (bronze+silver+gold)
    python pipeline.py silver gold  # runs only selected layers
    
Idempotency strategy:
    Each layer uses TRUNCATE + INSERT instead of DROP + CREATE.
    This preserves table schema and indexes while replacing data safely.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
import logging
import sys
import os
import psycopg2

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
DB_URL = "jdbc:postgresql://localhost:5432/flights_db"
DB_PROPS = {
    "user":     "admin",
    "password": "admin123",
    "driver":   "org.postgresql.Driver",
    "truncate": "true",   # TRUNCATE instead of DROP+CREATE
}

PG_CONN = {
    "host":     "localhost",
    "port":     5432,
    "database": "flights_db",
    "user":     "admin",
    "password": "admin123",
}

RAW_CSV = os.path.join(os.path.dirname(__file__), "../../data/raw/flights_sample_3m.csv")


def get_spark() -> SparkSession:
    """Create or reuse a SparkSession with the PostgreSQL JDBC driver."""
    jar_path = os.path.join(
        os.path.dirname(__file__),
        "../../jars/postgresql-42.7.3.jar"
    )
    return (
        SparkSession.builder
        .appName("FlightsDataPipeline")
        .config("spark.jars", jar_path)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )


def pg_execute(sql: str) -> None:
    """Execute a SQL statement directly via psycopg2 (DDL, TRUNCATE etc.)."""
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()


def write_idempotent(df: DataFrame, schema: str, table: str) -> None:
    """
    Write DataFrame to PostgreSQL using TRUNCATE + INSERT (idempotent).
    Creates table on first run, truncates on subsequent runs.
    Never drops the table - preserves schema and indexes.
    """
    full_table = f"{schema}.{table}"
    df.write.jdbc(
        url=DB_URL,
        table=full_table,
        mode="overwrite",
        properties=DB_PROPS,   # truncate=true tells JDBC to TRUNCATE not DROP
    )


# ── Bronze ─────────────────────────────────────────────────────────────────────
def load_bronze(spark: SparkSession) -> None:
    """Load raw CSV 1:1 into bronze.flights (idempotent TRUNCATE + INSERT)."""
    log.info("BRONZE – ensuring schema exists …")
    pg_execute("CREATE SCHEMA IF NOT EXISTS bronze;")

    log.info("BRONZE – reading CSV …")
    df = spark.read.csv(RAW_CSV, header=True, inferSchema=False)
    log.info(f"BRONZE – {df.count():,} rows read from CSV")

    log.info("BRONZE – writing to PostgreSQL (TRUNCATE + INSERT) …")
    write_idempotent(df, "bronze", "flights")
    log.info("BRONZE – done ✅")


# ── Silver ─────────────────────────────────────────────────────────────────────
def load_silver(spark: SparkSession) -> None:
    """Clean CSV data and write to silver.flights (idempotent TRUNCATE + INSERT)."""
    log.info("SILVER – ensuring schema exists …")
    pg_execute("CREATE SCHEMA IF NOT EXISTS silver;")

    log.info("SILVER – reading directly from CSV …")
    df = spark.read.csv(RAW_CSV, header=True, inferSchema=False)

    log.info("SILVER – cleaning …")
    df_clean = (
        df
        .dropDuplicates()
        .filter(
            F.col("FL_DATE").isNotNull() &
            F.col("AIRLINE").isNotNull() &
            F.col("ORIGIN").isNotNull() &
            F.col("DEST").isNotNull() &
            F.col("FL_DATE").rlike(r"^\d{4}-\d{2}-\d{2}$")
        )
        .select(
            F.to_date("FL_DATE", "yyyy-MM-dd").alias("fl_date"),
            F.col("AIRLINE").alias("airline"),
            F.col("AIRLINE_CODE").alias("airline_code"),
            F.col("FL_NUMBER").cast(IntegerType()).alias("fl_number"),
            F.col("ORIGIN").alias("origin"),
            F.col("ORIGIN_CITY").alias("origin_city"),
            F.col("DEST").alias("dest"),
            F.col("DEST_CITY").alias("dest_city"),
            F.col("CRS_DEP_TIME").cast(IntegerType()).alias("crs_dep_time"),
            F.when(F.col("DEP_TIME") == "NaN", None).otherwise(F.col("DEP_TIME").cast(FloatType())).alias("dep_time"),
            F.when(F.col("DEP_DELAY") == "NaN", None).otherwise(F.col("DEP_DELAY").cast(FloatType())).alias("dep_delay"),
            F.col("CRS_ARR_TIME").cast(IntegerType()).alias("crs_arr_time"),
            F.when(F.col("ARR_TIME") == "NaN", None).otherwise(F.col("ARR_TIME").cast(FloatType())).alias("arr_time"),
            F.when(F.col("ARR_DELAY") == "NaN", None).otherwise(F.col("ARR_DELAY").cast(FloatType())).alias("arr_delay"),
            F.when(F.col("DISTANCE") == "NaN", None).otherwise(F.col("DISTANCE").cast(FloatType())).alias("distance"),
            F.when(F.col("AIR_TIME") == "NaN", None).otherwise(F.col("AIR_TIME").cast(FloatType())).alias("air_time"),
            F.when(F.col("ELAPSED_TIME") == "NaN", None).otherwise(F.col("ELAPSED_TIME").cast(FloatType())).alias("elapsed_time"),
            F.when(F.col("DELAY_DUE_CARRIER") == "NaN", None).otherwise(F.col("DELAY_DUE_CARRIER").cast(FloatType())).alias("delay_due_carrier"),
            F.when(F.col("DELAY_DUE_WEATHER") == "NaN", None).otherwise(F.col("DELAY_DUE_WEATHER").cast(FloatType())).alias("delay_due_weather"),
            F.when(F.col("DELAY_DUE_NAS") == "NaN", None).otherwise(F.col("DELAY_DUE_NAS").cast(FloatType())).alias("delay_due_nas"),
            F.when(F.col("DELAY_DUE_SECURITY") == "NaN", None).otherwise(F.col("DELAY_DUE_SECURITY").cast(FloatType())).alias("delay_due_security"),
            F.when(F.col("DELAY_DUE_LATE_AIRCRAFT") == "NaN", None).otherwise(F.col("DELAY_DUE_LATE_AIRCRAFT").cast(FloatType())).alias("delay_due_late_aircraft"),
            F.when(F.col("CANCELLED") == "NaN", None).otherwise(F.col("CANCELLED").cast(FloatType())).alias("cancelled"),
            F.col("CANCELLATION_CODE").alias("cancellation_code"),
            F.when(F.col("DIVERTED") == "NaN", None).otherwise(F.col("DIVERTED").cast(FloatType())).alias("diverted"),
        )
    )

    log.info("SILVER – writing to PostgreSQL (TRUNCATE + INSERT) …")
    write_idempotent(df_clean, "silver", "flights")
    log.info("SILVER – done ✅")


# ── Gold ───────────────────────────────────────────────────────────────────────
def load_gold(spark: SparkSession) -> None:
    """Aggregate CSV data into gold.airline_route_stats (idempotent TRUNCATE + INSERT)."""
    log.info("GOLD – ensuring schema exists …")
    pg_execute("CREATE SCHEMA IF NOT EXISTS gold;")

    log.info("GOLD – reading directly from CSV …")
    df = spark.read.csv(RAW_CSV, header=True, inferSchema=False)

    log.info("GOLD – cleaning and aggregating …")
    df_gold = (
        df
        .filter(
            F.col("FL_DATE").isNotNull() &
            F.col("AIRLINE").isNotNull() &
            F.col("ORIGIN").isNotNull() &
            F.col("DEST").isNotNull()
        )
        .groupBy("AIRLINE", "AIRLINE_CODE", "ORIGIN", "ORIGIN_CITY", "DEST", "DEST_CITY")
        .agg(
            F.count("*").alias("total_flights"),
            F.round(F.avg(F.when(F.col("DEP_DELAY") == "NaN", None).otherwise(F.col("DEP_DELAY").cast(FloatType()))), 2).alias("avg_dep_delay_min"),
            F.round(F.avg(F.when(F.col("ARR_DELAY") == "NaN", None).otherwise(F.col("ARR_DELAY").cast(FloatType()))), 2).alias("avg_arr_delay_min"),
            F.round(F.avg(F.when(F.col("DISTANCE") == "NaN", None).otherwise(F.col("DISTANCE").cast(FloatType()))), 2).alias("avg_distance_miles"),
            F.round(F.avg(F.when(F.col("DELAY_DUE_CARRIER") == "NaN", None).otherwise(F.col("DELAY_DUE_CARRIER").cast(FloatType()))), 2).alias("avg_carrier_delay"),
            F.round(F.avg(F.when(F.col("DELAY_DUE_WEATHER") == "NaN", None).otherwise(F.col("DELAY_DUE_WEATHER").cast(FloatType()))), 2).alias("avg_weather_delay"),
            F.round(F.avg(F.when(F.col("DELAY_DUE_NAS") == "NaN", None).otherwise(F.col("DELAY_DUE_NAS").cast(FloatType()))), 2).alias("avg_nas_delay"),
            F.round(F.avg(F.when(F.col("DELAY_DUE_LATE_AIRCRAFT") == "NaN", None).otherwise(F.col("DELAY_DUE_LATE_AIRCRAFT").cast(FloatType()))), 2).alias("avg_late_aircraft_delay"),
            F.round(F.sum(F.when(F.col("CANCELLED") == "NaN", None).otherwise(F.col("CANCELLED").cast(FloatType()))), 0).alias("total_cancelled"),
            F.round(100.0 * F.sum(F.when(F.col("CANCELLED") == "NaN", None).otherwise(F.col("CANCELLED").cast(FloatType()))) / F.count("*"), 2).alias("cancellation_rate_pct"),
            F.min(F.to_date("FL_DATE", "yyyy-MM-dd")).alias("first_flight_date"),
            F.max(F.to_date("FL_DATE", "yyyy-MM-dd")).alias("last_flight_date"),
        )
        .filter(F.col("total_flights") >= 10)
        .toDF(
            "airline", "airline_code", "origin", "origin_city", "dest", "dest_city",
            "total_flights", "avg_dep_delay_min", "avg_arr_delay_min", "avg_distance_miles",
            "avg_carrier_delay", "avg_weather_delay", "avg_nas_delay", "avg_late_aircraft_delay",
            "total_cancelled", "cancellation_rate_pct", "first_flight_date", "last_flight_date"
        )
    )

    log.info("GOLD – writing to PostgreSQL (TRUNCATE + INSERT) …")
    write_idempotent(df_gold, "gold", "airline_route_stats")
    log.info("GOLD – done ✅")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    layers = sys.argv[1:] if len(sys.argv) > 1 else ["bronze", "silver", "gold"]

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if "bronze" in layers:
            load_bronze(spark)
        if "silver" in layers:
            load_silver(spark)
        if "gold" in layers:
            load_gold(spark)
    finally:
        spark.stop()
        log.info("Pipeline complete 🚀")