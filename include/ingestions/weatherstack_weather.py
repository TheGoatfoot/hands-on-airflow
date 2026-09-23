import json
import logging
from pathlib import Path

import pendulum
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DDL_FILE_PATH = (
    Path(__file__).resolve().parents[1] / "sql" / "raw_weatherstack_weather.sql"
)


def init_raw_weatherstack_weather_table(
    postgres_conn_id: str = "postgres_dw",
) -> None:
    """Ensure the raw_weatherstack_weather landing table exists in PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with open(DDL_FILE_PATH, encoding="utf-8") as f:
        ddl = f.read()
    hook.run(ddl)
    logger.info("Successfully ensured raw_weatherstack_weather table exists.")


def fetch_and_load_weatherstack_weather(
    city: str,
    ds: str,
    http_conn_id: str = "weatherstack_api",
    postgres_conn_id: str = "postgres_dw",
    **context,
) -> None:
    """Fetch weather observation from Weatherstack API and load raw JSON into PostgreSQL.

    For today's date, queries the /current endpoint.
    For past dates (backfill), queries the /historical endpoint.
    """
    now_utc7 = pendulum.now("Asia/Jakarta").to_date_string()
    is_current = ds >= now_utc7

    hook = HttpHook(http_conn_id=http_conn_id, method="GET")
    conn = hook.get_connection(http_conn_id)
    api_key = conn.password

    if is_current:
        endpoint = "current"
        params = {"access_key": api_key, "query": city}
        logger.info(
            "Fetching current weatherstack observation for city=%s on date=%s",
            city,
            ds,
        )
    else:
        endpoint = "historical"
        params = {
            "access_key": api_key,
            "query": city,
            "historical_date": ds,
        }
        logger.info(
            "Fetching historical weatherstack observation for city=%s on date=%s",
            city,
            ds,
        )

    response = hook.run(endpoint=endpoint, data=params)
    data = response.json()

    # Check for Weatherstack API error responses
    if data.get("success") is False or "error" in data:
        err = data.get("error", {})
        code = err.get("code")
        err_type = err.get("type")
        info = err.get("info")
        raise RuntimeError(
            f"Weatherstack API Error [{code} - {err_type}]: {info} "
            f"(endpoint={endpoint}, city={city}, date={ds})"
        )

    # Idempotently upsert raw payload into PostgreSQL
    upsert_sql = """
        INSERT INTO raw_weatherstack_weather (city, observation_date, payload, ingested_at)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (city, observation_date)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            ingested_at = CURRENT_TIMESTAMP;
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_hook.run(
        upsert_sql,
        parameters=(city, ds, json.dumps(data)),
    )
    logger.info(
        "Successfully loaded raw_weatherstack_weather payload for %s on %s",
        city,
        ds,
    )

