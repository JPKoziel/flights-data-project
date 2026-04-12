"""
Flights Data Pipeline DAG
Orchestrates the Bronze -> Silver -> Gold medallion pipeline.

Schedule: daily at 6:00 AM
Single entry point: triggers scripts/spark/pipeline.py with selected layers.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ── Config ─────────────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..")
VENV_PYTHON  = os.path.join(PROJECT_DIR, "venv/bin/python3")
PIPELINE     = os.path.join(PROJECT_DIR, "scripts/spark/pipeline.py")
JAVA_HOME    = "/opt/homebrew/opt/openjdk@17"

default_args = {
    "owner":            "flights-team",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="flights_medallion_pipeline",
    description="Bronze → Silver → Gold pipeline for US flight delays",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",   # daily at 06:00
    catchup=False,
    tags=["flights", "medallion", "pyspark"],
) as dag:

    # ── Task 1: Bronze ─────────────────────────────────────────────────────────
    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=(
            f"export JAVA_HOME={JAVA_HOME} && "
            f"export PATH=$JAVA_HOME/bin:$PATH && "
            f"{VENV_PYTHON} {PIPELINE} bronze"
        ),
        doc_md="""
        ## Load Bronze Layer
        Reads raw CSV from `data/raw/flights_sample_3m.csv` and loads it
        1:1 into `bronze.flights` (all columns as TEXT).
        Idempotent: overwrites existing data.
        """,
    )

    # ── Task 2: Silver ─────────────────────────────────────────────────────────
    load_silver = BashOperator(
        task_id="load_silver",
        bash_command=(
            f"export JAVA_HOME={JAVA_HOME} && "
            f"export PATH=$JAVA_HOME/bin:$PATH && "
            f"{VENV_PYTHON} {PIPELINE} silver"
        ),
        doc_md="""
        ## Load Silver Layer
        Reads CSV, cleans data (NaN → NULL, type casting, deduplication)
        and writes to `silver.flights`.
        Idempotent: overwrites existing data.
        """,
    )

    # ── Task 3: Gold ───────────────────────────────────────────────────────────
    load_gold = BashOperator(
        task_id="load_gold",
        bash_command=(
            f"export JAVA_HOME={JAVA_HOME} && "
            f"export PATH=$JAVA_HOME/bin:$PATH && "
            f"{VENV_PYTHON} {PIPELINE} gold"
        ),
        doc_md="""
        ## Load Gold Layer
        Aggregates flight data by airline + route into
        `gold.airline_route_stats` with avg delays and cancellation rates.
        Idempotent: overwrites existing data.
        """,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    load_bronze >> load_silver >> load_gold