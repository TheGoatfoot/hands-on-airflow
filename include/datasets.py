"""Airflow Dataset definitions for event-driven, data-aware pipeline orchestration.

Naming Convention (Domain-Driven Abstract URI):
    dataset://<domain>/<entity>

Benefits:
    - 100% environment-agnostic (identical across local dev, staging, and prod)
    - Decoupled from physical database hostnames and connection credentials
    - Conforms to RFC 3986 and Airflow 2.9+ / AIP-60 dataset standards
"""

from airflow.datasets import Dataset

# Domain: weather
RAW_OPENMETEO_WEATHER_DATASET = Dataset("dataset://weather/raw_openmeteo_weather")
RAW_WEATHERSTACK_WEATHER_DATASET = Dataset("dataset://weather/raw_weatherstack_weather")
