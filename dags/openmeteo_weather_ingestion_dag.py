import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

# Ensure project root is accessible for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from include.datasets import RAW_OPENMETEO_WEATHER_DATASET
from include.ingestions.openmeteo_weather import (
    fetch_and_load_openmeteo_weather,
    init_raw_openmeteo_weather_table,
)

# Timezone: UTC+7 (Asia/Jakarta)
LOCAL_TZ = pendulum.timezone("Asia/Jakarta")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="openmeteo_weather_ingestion_dag",
    default_args=default_args,
    description="Ingest daily weather observations from Open-Meteo Historical Weather API at 12 PM UTC+7 into PostgreSQL DW",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Jakarta"),
    catchup=True,
    max_active_runs=1,
    tags=["openmeteo", "weather", "ingestion", "raw"],
)
def openmeteo_weather_ingestion_dag():
    @task(task_id="init_raw_openmeteo_weather_table")
    def init_table():
        init_raw_openmeteo_weather_table()

    @task(task_id="ingest_openmeteo_weather", outlets=[RAW_OPENMETEO_WEATHER_DATASET])
    def ingest_weather(ds: str | None = None):
        fetch_and_load_openmeteo_weather(
            city="Jakarta",
            latitude=-6.2146,
            longitude=106.8451,
            ds=ds,
        )

    init_table() >> ingest_weather()


openmeteo_weather_ingestion_dag()
