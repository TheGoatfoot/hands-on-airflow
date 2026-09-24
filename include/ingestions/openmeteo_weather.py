import json
import logging
from pathlib import Path

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DDL_FILE_PATH = Path(__file__).resolve().parents[1] / "sql" / "openmeteo_weather.sql"


def init_raw_openmeteo_weather_table(
    postgres_conn_id: str = "postgres_dw",
) -> None:
    """Ensure the raw.openmeteo_weather landing table exists in PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with open(DDL_FILE_PATH, encoding="utf-8") as f:
        ddl = f.read()
    hook.run(ddl)
    logger.info("Successfully ensured raw.openmeteo_weather table exists.")


def fetch_and_load_openmeteo_weather(
    city: str,
    latitude: float,
    longitude: float,
    ds: str,
    http_conn_id: str = "openmeteo_api",
    postgres_conn_id: str = "postgres_dw",
    **context,
) -> None:
    """Fetch daily weather observations from Open-Meteo Historical Weather API and load JSON into PostgreSQL."""
    hook = HttpHook(http_conn_id=http_conn_id, method="GET")

    daily_vars = (
        "weather_code,temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
        "apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,"
        "sunrise,sunset,daylight_duration,sunshine_duration,"
        "precipitation_sum,rain_sum,precipitation_hours,"
        "wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,"
        "shortwave_radiation_sum,et0_fao_evapotranspiration"
    )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": ds,
        "end_date": ds,
        "timezone": "Asia/Jakarta",
        "daily": daily_vars,
    }

    logger.info(
        "Fetching Open-Meteo historical weather for city=%s (lat=%s, lon=%s) on date=%s",
        city,
        latitude,
        longitude,
        ds,
    )

    response = hook.run(endpoint="v1/archive", data=params)
    data = response.json()

    if data.get("error") is True:
        reason = data.get("reason", "Unknown API error")
        raise RuntimeError(f"Open-Meteo API Error: {reason} (city={city}, date={ds})")

    upsert_sql = """
        INSERT INTO raw.openmeteo_weather (city, observation_date, latitude, longitude, payload, ingested_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (city, observation_date)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            payload = EXCLUDED.payload,
            ingested_at = CURRENT_TIMESTAMP;
    """

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_hook.run(
        upsert_sql,
        parameters=(city, ds, latitude, longitude, json.dumps(data)),
    )
    logger.info(
        "Successfully loaded raw.openmeteo_weather payload for %s on %s",
        city,
        ds,
    )
