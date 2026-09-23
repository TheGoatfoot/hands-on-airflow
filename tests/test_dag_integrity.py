from pathlib import Path
import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_FILES = [f for f in DAGS_DIR.glob("*.py") if not f.name.startswith("__")]


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.name)
def test_dag_file_imports_cleanly(dag_file: Path):
    """Ensure that each DAG file parses cleanly through Airflow's native DagBag."""
    dagbag = DagBag(dag_folder=str(dag_file))

    # 1. Assert no import, syntax, or parsing errors occurred
    assert len(dagbag.import_errors) == 0, f"Import errors in {dag_file.name}: {dagbag.import_errors}"

    # 2. File must contain at least one DAG object
    assert len(dagbag.dags) > 0, f"No DAG instance found in {dag_file.name}"

    # 3. Check for circular dependencies
    for dag in dagbag.dags.values():
        dag.check_cycle()
