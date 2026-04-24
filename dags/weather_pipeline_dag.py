"""
Weather Data Pipeline DAG
==========================
Runs the weather extraction script once per day at 06:00 (Asia/Colombo).

DAG ID  : weather_data_pipeline
Schedule: @daily  (0 6 * * *)
Owner   : student
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the extraction module that lives in /opt/airflow/scripts/
from extract_weather import run_pipeline

# ── Default arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner":            "student",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="weather_data_pipeline",
    default_args=default_args,
    description="Daily weather data extraction for Colombo stored in PostgreSQL",
    schedule_interval="0 6 * * *",          # 06:00 every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "etl", "postgresql"],
) as dag:

    # ── Task 1 – Log pipeline start ───────────────────────────────────────────
    def log_start(**context):
        logging.info("=" * 60)
        logging.info("Weather Pipeline STARTED")
        logging.info("Execution date : %s", context["ds"])
        logging.info("Run ID         : %s", context["run_id"])
        logging.info("=" * 60)

    task_start = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_start,
    )

    # ── Task 2 – Extract & Load ────────────────────────────────────────────────
    def extract_and_load(**context):
        logging.info("Starting weather data extraction …")
        run_pipeline()
        logging.info("Extraction and load complete.")

    task_extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_and_load,
    )

    # ── Task 3 – Log pipeline end ─────────────────────────────────────────────
    def log_end(**context):
        logging.info("=" * 60)
        logging.info("Weather Pipeline COMPLETED SUCCESSFULLY")
        logging.info("Execution date : %s", context["ds"])
        logging.info("=" * 60)

    task_end = PythonOperator(
        task_id="log_pipeline_end",
        python_callable=log_end,
    )

    # ── Task dependency chain ─────────────────────────────────────────────────
    task_start >> task_extract >> task_end
