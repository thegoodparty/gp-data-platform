"""Behavioural assertions for the Bifrost loader DAG.

Loaded from the file path directly rather than the configured dags_folder, matching
test_gold_match_daily_dag.py: CI does not point dags_folder at astro/dags, and building the
DagBag at collection time keeps this on real Airflow with no metastore dependency.

These call the task callables rather than reading the DAG file as text. A source-grep test
passes just as happily against a DAG that does nothing.
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

DagBag = pytest.importorskip("airflow.models").DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "load_bifrost_usage.py")

# The names the DAG reads, with values that pass the loader's identifier validation: a dry run
# builds the statement for real, so `v-<name>` would fail it for the right reason.
_VARIABLES = {
    "databricks_catalog": "goodparty_data_catalog",
    "bifrost_landing_schema": "airflow_source",
    "bifrost_export_bucket": "bucket",
    "bifrost_export_prefix": "bifrost/logs",
}
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("load_bifrost_usage")


def _callable(task_id):
    """A task's callable.

    The DagBag imports the DAG file itself, so patching a module imported by the test would hit a
    different instance. Patching `_callable(...).__globals__` targets the dict the function
    actually resolves names from, which is correct however the module was loaded.
    """
    operator = _DAG.get_task(task_id)
    callable_ = getattr(operator, "python_callable", None)
    assert callable_ is not None, f"{task_id} has no python callable"
    return callable_


def test_dag_loads():
    assert _DAG is not None, f"load_bifrost_usage failed to load from {_DAG_FILE}"


def test_serialised_and_paused_on_creation():
    """`max_active_runs=1` so two runs cannot race COPY INTO's own bookkeeping.
    `is_paused_upon_creation=True` because BUILD must not schedule anything: the export does not
    exist yet, so an unpaused DAG would just fail nightly until ops delivers."""
    assert _DAG.max_active_runs == 1
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_runs_after_the_export():
    assert _DAG.timetable.expression == "30 10 * * *"


def test_one_load_task_and_no_aws_dependency():
    """COPY INTO owns the file bookkeeping, so there is no listing task and no per-day mapped
    task. Nothing here lists or reads S3, which is what keeps Astro out of the engineer's
    bucket."""
    assert sorted(task.task_id for task in _DAG.tasks) == ["load", "report"]

    source = Path(_DAG_FILE).read_text()
    for forbidden in ("boto3", "head_object", "botocore", "list_objects", "hive_partition"):
        assert forbidden not in source, f"{forbidden} would need AWS access for Airflow"


def test_a_failed_load_fails_the_run():
    """`report` is the only leaf, and it must not run on a failed load: a green run that landed
    nothing is the failure this loader exists to avoid."""
    assert _DAG.get_task("report").trigger_rule == "all_success"


def test_load_creates_the_table_before_it_lands():
    """The DAG owns the landing table. If this ordering inverted, the first COPY INTO would fail
    on a missing relation."""
    calls = []
    connection = MagicMock()
    connection.__enter__ = MagicMock(return_value=connection)
    connection.__exit__ = MagicMock(return_value=False)

    load = _callable("load")
    globals_ = load.__globals__

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": MagicMock(return_value=connection),
                "ensure_landing_table": lambda *a, **k: calls.append("ensure"),
                "land_export": lambda *a, **k: (calls.append("land"), 7)[1],
            },
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(get=Mock(side_effect=lambda name, **kw: f"v-{name}")),
                "get_current_context": Mock(return_value={"params": {}}),
            },
        ),
    ):
        loaded = load()

    assert calls == ["ensure", "land"]
    assert loaded == 7


def test_load_resolves_the_databricks_connection_by_variable_name():
    """`connect_from_conn_id` does its own Variable lookup; passing it a resolved value makes it
    treat a connection id as another Variable name and fails before the DDL."""
    connection = MagicMock()
    connection.__enter__ = MagicMock(return_value=connection)
    connection.__exit__ = MagicMock(return_value=False)

    load = _callable("load")
    globals_ = load.__globals__
    connect = MagicMock(return_value=connection)

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": connect,
                "ensure_landing_table": MagicMock(),
                "land_export": MagicMock(return_value=0),
            },
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(get=Mock(return_value="x")),
                "get_current_context": Mock(return_value={"params": {}}),
            },
        ),
    ):
        load()

    connect.assert_called_once_with()


def test_dry_run_builds_the_statement_and_touches_nothing():
    """A dry run must not open a connection: it exists to show what would run."""
    connect = MagicMock()
    load = _callable("load")
    globals_ = load.__globals__

    with (
        patch.dict(globals_, {"connect_from_conn_id": connect}),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(get=Mock(side_effect=lambda name, **kw: _VARIABLES[name])),
                "get_current_context": Mock(return_value={"params": {"dry_run": True}}),
            },
        ),
    ):
        loaded = load()

    connect.assert_not_called()
    assert loaded == 0
