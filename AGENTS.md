# AGENTS.md

Instructions, conventions, and context for AI coding agents working on this Apache Airflow and dbt data pipeline repository.

---

## 1. Project Overview & Architecture

This project is an ELT / ETL data pipeline orchestrated with **Apache Airflow**, transformed via **dbt** using **Astronomer Cosmos**, writing to a **PostgreSQL Data Warehouse**.

- **Airflow Orchestrator**: Apache Airflow 2.9.3 running via Docker Compose (`docker-compose.yaml`).
- **Transformation Engine**: **dbt** (`dbt-postgres`) orchestrated into native Airflow tasks via **Astronomer Cosmos** (`astronomer-cosmos`).
- **Python Tooling & Virtualenv**: Managed locally via **`uv`** (Python 3.12).
- **Airflow Metadata Database**: PostgreSQL 16 container (`airflow-postgres`).
- **Target Data Warehouse**: PostgreSQL 16 container (`dw-postgres`).
  - Container hostname: `dw-postgres:5432`
  - Host port mapping: `localhost:5433` (database and credentials configured via `.env`)
  - Airflow Connection ID: `postgres_dw` (configured in `docker-compose.yaml` via `AIRFLOW_CONN_POSTGRES_DW`)
- **Airflow Web UI**: `http://localhost:8080` (admin credentials configured via `.env`).

---

## 2. Directory Structure Conventions

Maintain the following structure for files and modules:

```text
hands-on-airflow/
├── dags/                  # Airflow DAG definitions ONLY (Thin DAGs)
│   ├── dbt_transform_dag.py
│   └── *.py
├── dbt/                   # dbt project root
│   ├── dbt_project.yml    # dbt project configuration
│   ├── profiles.yml       # Dual host/container connection profile
│   ├── models/            # SQL transformation models
│   │   ├── staging/       # Views: light cleaning & casting over raw/seeds
│   │   ├── marts/         # Tables/Incremental: dimensional & fact models
│   │   └── schema.yml     # Model documentation & data quality tests
│   └── seeds/             # Static lookup and test seed datasets
├── plugins/               # Custom Airflow operators, hooks, sensors, and macros
├── include/               # Modular business logic, SQL files, custom transformations
│   ├── sql/               # SQL queries / DDL for Data Warehouse
│   └── transformations/   # Python extraction and load functions
├── tests/                 # Unit tests & DagBag integrity checks
│   ├── test_dag_integrity.py
│   └── ...
├── config/                # Airflow configuration overrides
├── logs/                  # Airflow task execution logs (ignored in git)
├── docker-compose.yaml    # Multi-container setup for Airflow & Postgres DW
├── pyproject.toml         # Python dependencies & test configuration via uv
├── uv.lock                # Dependency lockfile
└── AGENTS.md              # Agent guidelines and operating instructions
```

---

## 3. Airflow & dbt Best Practices

### 3.1. "Thin DAG" Principle (Crucial)
Airflow DAGs must strictly act as **orchestrators, not execution engines**.
- **No heavy business logic in `dags/`**: Never place complex data processing, API calls, or heavy SQL transformations directly in the DAG file.
- **Separation of concerns**:
  - Keep DAG definitions in `dags/` minimal: define DAG settings, schedules, operator instantiation, and dependency wiring (`>>`).
  - Use **Astronomer Cosmos** (`cosmos.DbtTaskGroup` or `cosmos.DbtDag`) to delegate SQL modeling, testing, and DAG lineage directly to dbt.
  - Put non-dbt extract/load logic in `include/` or `plugins/`.

### 3.2. Cosmos & dbt Authoring Guidelines
- **Dual-Path Project Resolution**: Always configure the dbt project path to resolve both inside Docker (`/opt/airflow/dbt`) and on the local host:
  ```python
  if os.path.exists("/opt/airflow/dbt"):
      DBT_PROJECT_PATH = Path("/opt/airflow/dbt")
  else:
      DBT_PROJECT_PATH = Path(__file__).resolve().parents[1] / "dbt"
  ```
- **Dual-Environment `profiles.yml`**: `dbt/profiles.yml` uses environment variables with defaults to allow identical usage across environments:
  - **Host**: defaults to `localhost` and port `5433`.
  - **Docker**: reads `DBT_HOST=dw-postgres` and `DBT_PORT=5432` from `docker-compose.yaml`.
- **Layered Modeling**:
  - `staging/`: Views (`+materialized: view`) performing 1:1 cleaning and casting.
  - `marts/`: Tables or incremental models (`+materialized: table`) combining staging models for downstream consumption.
- **Data Quality Tests**: Every new model must have schema documentation and constraint tests (`unique`, `not_null`) defined in `schema.yml`.

### 3.3. Avoid Top-Level Code & Parse-Time Overhead
The Airflow scheduler parses every file in `dags/` periodically (every 30s as per `AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL`):
- **NO top-level DB queries**: Never open connections or query databases at module scope in a DAG file.
- **NO top-level Airflow Variables or Connections**: Do NOT call `Variable.get()` or `Connection.get()` at module scope.
- **Lazy imports**: Import heavy libraries (e.g. `pandas`, `polars`, `numpy`) inside the specific task function, not at top level.

### 3.4. Idempotency & Determinism
- Every task must be **idempotent**: running the task multiple times for the same execution window must produce the same result without duplicate data or corrupted state.
- dbt models should leverage proper materialization (`view`, `table`, or `incremental` with `unique_key`).
- Rely on Airflow's built-in execution context (`logical_date`, `ds`, `data_interval_start`) rather than non-deterministic calls like `datetime.now()`.

---

## 4. Development Environment & `uv` Commands

Local development, dependency management, and testing are done via **`uv`**.

### Dependencies Management
- Sync virtual environment:
  ```bash
  uv sync
  ```
- Add a runtime dependency:
  ```bash
  uv add <package-name>
  ```
- Add development/test dependencies:
  ```bash
  uv add --dev pytest pytest-env ruff
  ```
- Run Python commands/scripts inside the virtual environment:
  ```bash
  uv run python <script.py>
  ```

### Local dbt Workflows (Against Host Port 5433)
Ensure the warehouse container is running (`docker compose up -d dw-postgres`), then run dbt commands locally with `--env-file .env`:
- Test connection to PostgreSQL Data Warehouse:
  ```bash
  uv run --env-file .env dbt debug --project-dir dbt --profiles-dir dbt
  ```
- Load seed data:
  ```bash
  uv run --env-file .env dbt seed --project-dir dbt --profiles-dir dbt
  ```
- Run transformation models:
  ```bash
  uv run --env-file .env dbt run --project-dir dbt --profiles-dir dbt
  ```
- Execute data quality tests:
  ```bash
  uv run --env-file .env dbt test --project-dir dbt --profiles-dir dbt
  ```

---

## 5. Docker & Infrastructure Commands

### Service Management
- Start Airflow and Postgres DW in detached mode:
  ```bash
  docker compose up -d
  ```
- Check container health and status:
  ```bash
  docker compose ps
  ```
- Follow scheduler logs:
  ```bash
  docker compose logs -f airflow-scheduler
  ```
- Follow webserver logs:
  ```bash
  docker compose logs -f airflow-webserver
  ```
- Stop services:
  ```bash
  docker compose down
  ```

### Container Dependency Injection
Airflow containers install Cosmos and dbt on startup via `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yaml`.
Exact versions are pinned to avoid pip resolution backtracking against Airflow 2.9.3:
```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "astronomer-cosmos==1.15.1 dbt-core==1.8.7 dbt-postgres==1.9.1 dbt-adapters==1.9.0 dbt-common==1.12.0"
```

### Data Warehouse Access
- Host access via `psql` (port 5433):
  ```bash
  PGPASSWORD="${DW_PASSWORD}" psql -h localhost -p "${DW_PORT_HOST:-5433}" -U "${DW_USER}" -d "${DW_DB:-warehouse}"
  ```
- Container-to-container access within Docker:
  - Host: `dw-postgres`
  - Port: `5432`
  - Database: `warehouse`
  - Connection ID: `postgres_dw` (credentials configured via `.env`)

---

## 6. Testing & Quality Assurance Instructions

### 6.1. DAG Integrity Testing (Local via `uv` & `pytest`)
Always verify DAG integrity using Airflow's native `DagBag` parser before committing:
```bash
uv run pytest tests/test_dag_integrity.py
```
> **Note**: `[tool.pytest.ini_options]` in `pyproject.toml` configures `AIRFLOW__COSMOS__ENABLE_CACHE=False` for test runs, allowing Cosmos DAGs to parse cleanly in local unit tests without requiring a running Airflow metadata database.

### 6.2. Testing DAGs inside Docker
To trigger an end-to-end test execution of a DAG directly within the scheduler container:
```bash
docker compose exec airflow-scheduler airflow dags test dbt_transform_dag 2026-01-01
```

To view the generated Cosmos task graph hierarchy:
```bash
docker compose exec airflow-scheduler airflow tasks list dbt_transform_dag --tree
```

### 6.3. Code Style & Linting
Run linter and formatter checks:
```bash
uv run ruff check .
uv run ruff format --check .
```

---

## 7. Git & Branching Workflow

### 7.1. Branch Creation & Isolation
- **Never commit directly to `main`**: All code, DAG, dbt model, or configuration changes must be made on a dedicated branch created from `main`.
- **Branch Naming Scheme**:
  `agent/<type>/<short-description>`
  - Types:
    - `agent/feature/*`: New DAGs, new dbt models, new operators, or major pipeline additions.
    - `agent/fix/*`: Bug fixes, DAG syntax/cycle repairs, broken SQL queries, failing tests.
    - `agent/refactor/*`: Code restructuring, moving logic to `include/`, cleaning models without behavior change.
    - `agent/infra/*`: Changes to `docker-compose.yaml`, `.env.example`, `pyproject.toml`, container dependencies.
    - `agent/test/*`: Adding or updating unit tests, DagBag integrity checks, dbt data tests.
    - `agent/docs/*`: Documentation changes, updating `AGENTS.md` or `README.md`.
    - `agent/perf/*`: Performance tuning (Cosmos caching, dbt incremental strategies, indexing).
  - Use lowercase letters and hyphens (e.g. `agent/feature/customer-orders-dag`).
- **Branch Creation Command**:
  ```bash
  git checkout main && git pull
  git checkout -b agent/<type>/<short-description>
  ```

### 7.2. Review & Merge Protocol
- **Do NOT merge into `main` automatically**: After completing work and verifying tests, commit the changes to the branch and present the branch name and a summary of changes to the user for review.
- **Merge ONLY when explicitly asked**: The agent must **only** merge its branch into `main` if the user explicitly instructs it to do so (e.g., "merge this branch", "merge into main"). Otherwise, leave the merge step to the user.

---

## 8. Rules for AI Agents

1. **Branch first**: Always create a new branch (`agent/<type>/<description>`) from `main` before modifying or creating files. Never commit directly to `main`.
2. **Merge only when asked**: Never merge into `main` unless the user explicitly asks you to merge.
3. **Verify DAG syntax & integrity**: Always run `uv run pytest tests/test_dag_integrity.py` after modifying or creating DAGs.
4. **Never hardcode database credentials**: Always reference `postgres_dw` connection ID, environment variables, or `dbt/profiles.yml`. Keep `.env` in `.gitignore`.
5. **Respect Thin DAG patterns**: Place SQL transformations under `dbt/models/`, keeping Airflow DAGs strictly for orchestrating tasks and dependencies.
6. **Test dbt changes locally**: Run `uv run dbt run` and `uv run dbt test` against the local warehouse before committing model changes.
7. **Ensure Idempotency**: Use staging views and table/incremental marts with unique keys so pipelines are safely re-runnable.

