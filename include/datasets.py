"""Airflow Dataset definitions for event-driven, data-aware pipeline orchestration."""

from airflow.datasets import Dataset

# Raw Ingestion Datasets (PostgreSQL DW landing tables)
RAW_OPENMETEO_WEATHER_DATASET = Dataset(
    "postgres://dw-postgres:5432/warehouse/public/raw_openmeteo_weather"
)

RAW_WEATHERSTACK_WEATHER_DATASET = Dataset(
    "postgres://dw-postgres:5432/warehouse/public/raw_weatherstack_weather"
)
