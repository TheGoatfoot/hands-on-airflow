import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

# Ensure project root is accessible for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from include.datasets import RAW_WEATHERSTACK_WEATHER_DATASET
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


@dag(
    dag_id="weatherstack_weather_ingestion_dag",
    default_args=default_args,
    description="Ingest daily weather observations from Weatherstack API at 12 PM UTC+7 into PostgreSQL DW",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 9, 23, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["weatherstack", "weather", "ingestion", "raw"],
)
def weatherstack_weather_ingestion_dag():
    @task(task_id="init_raw_weatherstack_weather_table")
    def init_table():
        init_raw_weatherstack_weather_table()

    @task(
        task_id="ingest_weatherstack_weather",
        outlets=[RAW_WEATHERSTACK_WEATHER_DATASET],
    )
    def ingest_weather(ds: str | None = None):
        fetch_and_load_weatherstack_weather(
            city="Jakarta",
            ds=ds,
        )

    init_table() >> ingest_weather()


weatherstack_weather_ingestion_dag()
