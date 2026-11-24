"""Behavioural assertions for the delegate swarm loader DAG.

Loaded from the file path directly rather than the configured dags_folder, matching
test_load_bifrost_usage_dag.py: CI does not point dags_folder at astro/dags, and building the DagBag
at collection time keeps this on real Airflow with no metastore dependency.

These call the task callables rather than reading the DAG file as text. A source-grep test passes
just as happily against a DAG that does nothing.
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


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "load_delegate_swarm_usage.py")

# The names the DAG reads, with values that pass the loader's identifier validation: a dry run builds
# the statements for real, so `v-<name>` would fail it for the right reason.
_VARIABLES = {
    "databricks_catalog": "goodparty_data_catalog",
    "delegate_swarm_landing_schema": "airflow_source",
    "delegate_swarm_export_bucket": "bucket",
    "delegate_swarm_cost_prefix": "delegate-swarm/session-cost",
    "delegate_swarm_task_edge_prefix": "delegate-swarm/task-edge",
}
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("load_delegate_swarm_usage")


def _callable(task_id):
    """A task's callable.

    The DagBag imports the DAG file itself, so patching a module imported by the test would hit a
    different instance. Patching `_callable(...).__globals__` targets the dict the function actually
    resolves names from, which is correct however the module was loaded.
    """
    operator = _DAG.get_task(task_id)
    callable_ = getattr(operator, "python_callable", None)
    assert callable_ is not None, f"{task_id} has no python callable"
    return callable_


def _connection():
    connection = MagicMock()
    connection.__enter__ = MagicMock(return_value=connection)
    connection.__exit__ = MagicMock(return_value=False)
    return connection


def test_dag_loads():
    assert _DAG is not None, f"load_delegate_swarm_usage failed to load from {_DAG_FILE}"


def test_serialised_and_paused_on_creation():
    """`max_active_runs=1` so two runs cannot race COPY INTO's own bookkeeping.
    `is_paused_upon_creation=True` because merging this must schedule nothing: the export lands in a
    prefix no external location is granted for yet, so an unpaused DAG would just fail nightly."""
    assert _DAG.max_active_runs == 1
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_runs_after_the_export_and_before_nothing_else():
    """The exporter runs at 09:15 UTC with a 30 minute bound, so 10:45 is clear of it, and clear of
    the gateway loader at 10:30."""
    assert _DAG.timetable.expression == "45 10 * * *"


def test_one_load_task_and_no_aws_dependency():
    """COPY INTO owns the file bookkeeping, so there is no listing task and no per-day mapped task.
    Nothing here lists or reads S3, which is what keeps Astro out of the engineer's bucket."""
    assert sorted(task.task_id for task in _DAG.tasks) == ["load", "report"]

    source = Path(_DAG_FILE).read_text()
    for forbidden in ("boto3", "head_object", "botocore", "list_objects", "hive_partition"):
        assert forbidden not in source, f"{forbidden} would need AWS access for Airflow"


def test_the_dag_has_a_dry_run_param():
    assert "dry_run" in {name for name in _DAG.params}


def test_a_failed_load_fails_the_run():
    """`report` is the only leaf, and it must not run on a failed load: a green run that landed
    nothing is the failure this loader exists to avoid."""
    assert _DAG.get_task("report").trigger_rule == "all_success"


def test_load_creates_both_tables_before_it_lands_anything():
    """The DAG owns the landing tables, and both have to exist before either file is read. If this
    ordering inverted, the first COPY INTO would fail on a missing relation."""
    calls = []
    connection = _connection()

    load = _callable("load")
    globals_ = load.__globals__

    def fake_land_exports(_connection, _catalog, _schema, _bucket, exports):
        calls.append(("land", [export.table.name for export in exports]))
        return {"delegate_swarm_session_cost": 3, "delegate_swarm_task_edge": 2}

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": MagicMock(return_value=connection),
                "land_exports": fake_land_exports,
            },
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(get=Mock(side_effect=lambda name, **kw: _VARIABLES[name])),
                "get_current_context": Mock(return_value={"params": {}}),
            },
        ),
    ):
        loaded = load()

    assert calls == [("land", ["delegate_swarm_session_cost", "delegate_swarm_task_edge"])]
    assert loaded == {"delegate_swarm_session_cost": 3, "delegate_swarm_task_edge": 2}


def test_load_reads_the_two_prefixes_from_their_own_variables():
    """One prefix per table because COPY INTO reads a whole prefix. Reading the same Variable twice
    would make each table's load read the other table's file."""
    connection = _connection()
    seen = {}

    def fake_land_exports(_connection, _catalog, _schema, _bucket, exports):
        seen["prefixes"] = [export.prefix for export in exports]
        return {}

    load = _callable("load")
    globals_ = load.__globals__

    with (
        patch.dict(
            globals_,
            {"connect_from_conn_id": MagicMock(return_value=connection), "land_exports": fake_land_exports},
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(get=Mock(side_effect=lambda name, **kw: _VARIABLES[name])),
                "get_current_context": Mock(return_value={"params": {}}),
            },
        ),
    ):
        load()

    assert seen["prefixes"] == [
        _VARIABLES["delegate_swarm_cost_prefix"],
        _VARIABLES["delegate_swarm_task_edge_prefix"],
    ]


def test_load_reads_the_landing_schema_from_its_own_variable():
    """`delegate_swarm_landing_schema` is what keeps a `dry_run=false` dev run off production data,
    the same way `bifrost_landing_schema` does. A DAG that read `databricks_catalog` or hardcoded
    `airflow_source` would pass every other test here."""
    connection = _connection()
    seen = {}

    def fake_land_exports(_connection, _catalog, schema, _bucket, _exports):
        seen["schema"] = schema
        return {}

    load = _callable("load")
    globals_ = load.__globals__

    with (
        patch.dict(
            globals_,
            {"connect_from_conn_id": MagicMock(return_value=connection), "land_exports": fake_land_exports},
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(
                    get=Mock(
                        side_effect=lambda name, **kw: {
                            **_VARIABLES,
                            "delegate_swarm_landing_schema": "airflow_source_dev",
                        }[name]
                    )
                ),
                "get_current_context": Mock(return_value={"params": {}}),
            },
        ),
    ):
        load()

    assert seen["schema"] == "airflow_source_dev"


def test_load_resolves_the_databricks_connection_by_variable_name():
    """`connect_from_conn_id` does its own Variable lookup; passing it a resolved value makes it
    treat a connection id as another Variable name and fails before the DDL."""
    connect = MagicMock(return_value=_connection())

    load = _callable("load")
    globals_ = load.__globals__

    with (
        patch.dict(
            globals_,
            {"connect_from_conn_id": connect, "land_exports": MagicMock(return_value={})},
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


def test_dry_run_logs_both_statements_and_touches_nothing(caplog):
    """A dry run must not open a connection: it exists to show what would run, and this is the one
    way to see both statements before the external location exists."""
    connect = MagicMock()
    land = MagicMock()
    load = _callable("load")
    globals_ = load.__globals__

    with (
        patch.dict(globals_, {"connect_from_conn_id": connect, "land_exports": land}),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(get=Mock(side_effect=lambda name, **kw: _VARIABLES[name])),
                "get_current_context": Mock(return_value={"params": {"dry_run": True}}),
            },
        ),
        caplog.at_level(logging.INFO, logger="airflow.task"),
    ):
        loaded = load()

    connect.assert_not_called()
    land.assert_not_called()
    assert loaded == {}

    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "delegate_swarm_session_cost" in logged
    assert "delegate_swarm_task_edge" in logged
    # Both statements are the real ones, built from the Variables.
    assert logged.count("copy into") == 2
    assert "s3://bucket/delegate-swarm/session-cost" in logged
    assert "s3://bucket/delegate-swarm/task-edge" in logged
    assert "fileformat = json" in logged
