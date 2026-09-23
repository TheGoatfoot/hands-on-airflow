import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is in sys.path for include imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from include.transformations.weather import (
    fetch_and_load_weather,
    init_raw_weather_table,
)

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_ingestion_dag",
    description="Ingests daily weather observations from Weatherstack into Postgres raw layer with 7-day backfill",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 16, tz=LOCAL_TZ),
    schedule="0 12 * * *",  # Everyday at 12:00 PM UTC+7
    catchup=True,
    tags=["weather", "ingestion", "raw"],
) as dag:
    init_table = PythonOperator(
        task_id="init_raw_weather_table",
        python_callable=init_raw_weather_table,
        op_kwargs={"postgres_conn_id": "postgres_dw"},
    )

    ingest_weather = PythonOperator(
        task_id="ingest_weather",
        python_callable=fetch_and_load_weather,
        op_kwargs={
            "city": "Jakarta",
            "ds": "{{ ds }}",
            "http_conn_id": "weatherstack_api",
            "postgres_conn_id": "postgres_dw",
        },
    )

    init_table >> ingest_weather
