import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Resolve path for both container (/opt/airflow/dbt) and local host development
if os.path.exists("/opt/airflow/dbt"):
    DBT_PROJECT_PATH = Path("/opt/airflow/dbt")
else:
    DBT_PROJECT_PATH = Path(__file__).resolve().parents[1] / "dbt"

# If environment variables are set (e.g. via .env or container env), use profiles.yml;
# otherwise fallback to Airflow Connection / Secrets Backend (conn_id: postgres_dw)
if os.environ.get("DBT_USER") and os.environ.get("DBT_PASSWORD"):
    profile_config = ProfileConfig(
        profile_name="dw_postgres",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
    )
else:
    profile_config = ProfileConfig(
        profile_name="dw_postgres",
        target_name="dev",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_dw",
            profile_args={"schema": os.environ.get("DBT_SCHEMA", "public")},
        ),
    )

with DAG(
    dag_id="dbt_transform_dag",
    description="Orchestrates dbt transformations against Postgres Data Warehouse using Cosmos",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger or triggered by upstream ingestion DAGs
    catchup=False,
    tags=["dbt", "transformation", "data_warehouse"],
) as dag:
    dbt_transforms = DbtTaskGroup(
        group_id="dbt_transforms",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
        ),
        render_config=RenderConfig(
            emit_datasets=False,
        ),
    )

