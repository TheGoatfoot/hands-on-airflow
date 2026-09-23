import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is accessible for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from include.ingestions.weatherstack_weather import (
    fetch_and_load_weatherstack_weather,
    init_raw_weatherstack_weather_table,
)

# Timezone: UTC+7 (Asia/Jakarta)
LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weatherstack_weather_ingestion_dag",
    default_args=default_args,
    description="Ingest daily weather observations from Weatherstack API at 12 PM UTC+7 into PostgreSQL DW",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 9, 16, tz="Asia/Jakarta"),
    catchup=True,
    max_active_runs=1,
    tags=["weatherstack", "weather", "ingestion", "raw"],
) as dag:
    init_table = PythonOperator(
        task_id="init_raw_weatherstack_weather_table",
        python_callable=init_raw_weatherstack_weather_table,
    )

    ingest_weather = PythonOperator(
        task_id="ingest_weatherstack_weather",
        python_callable=fetch_and_load_weatherstack_weather,
        op_kwargs={
            "city": "Jakarta",
            "ds": "{{ ds }}",
        },
    )

    init_table >> ingest_weather
