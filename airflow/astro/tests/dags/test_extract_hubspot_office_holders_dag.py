"""Behavioural assertions for the Office Holder companion extractor DAG.

Loaded from the file path directly rather than the configured dags_folder, matching
test_load_bifrost_usage_dag.py. These call the task callable rather than reading the DAG
file as text: a source-grep test passes just as happily against a DAG that does nothing.
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


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "extract_hubspot_office_holders.py")

with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("extract_hubspot_office_holders")


def _callable(task_id):
    """A task's callable, which resolves names in the DAG module's own globals."""
    operator = _DAG.get_task(task_id)
    callable_ = getattr(operator, "python_callable", None)
    assert callable_ is not None, f"{task_id} has no python callable"
    return callable_


def test_dag_loads():
    assert _DAG is not None, f"extract_hubspot_office_holders failed to load from {_DAG_FILE}"


def test_paused_on_creation_and_one_active_run():
    """Unpausing is the owner-gated activation step, and two concurrent full sweeps would
    land two snapshots for no benefit."""
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.max_active_runs == 1
    assert _DAG.catchup is False


def test_cadence_is_at_least_daily_and_not_baked_into_the_code():
    """The exact hour is an owner decision, so the cron comes from the deployment's
    environment and defaults to once a day."""
    assert _DAG.timetable.expression == "0 0 * * *"  # Airflow's rendering of @daily
    source = Path(_DAG_FILE).read_text()
    assert "HUBSPOT_OFFICE_HOLDERS_SCHEDULE" in source


def test_one_task_because_the_landing_is_one_snapshot():
    assert [task.task_id for task in _DAG.tasks] == ["extract"]


def test_the_run_id_combines_the_dag_run_and_the_attempt():
    """Staging reads the newest complete run, so the id has to identify one task attempt: a
    retry under the same dag run must not append a second copy under the id it then marks."""
    extract = _callable("extract")
    globals_ = extract.__globals__

    captured = {}

    def fake_extract(connection, session, token, catalog, schema, run_id, dry_run=False):
        captured.update(
            {
                "connection": connection,
                "session": session,
                "token": token,
                "catalog": catalog,
                "schema": schema,
                "run_id": run_id,
                "dry_run": dry_run,
            }
        )
        return {"run_id": run_id}

    connection = MagicMock()
    session = MagicMock()

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": MagicMock(return_value=connection),
                "make_session": MagicMock(return_value=session),
                "extract_office_holders": fake_extract,
            },
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(
                    get=Mock(
                        side_effect=lambda name, **kw: {
                            "hubspot_companion_loader_token": "tok",
                            "hubspot_companion_landing_schema": "raw.hubspot",
                        }[name]
                    )
                ),
                "get_current_context": Mock(
                    return_value={
                        "params": {"dry_run": False},
                        "dag_run": Mock(run_id="scheduled__1"),
                        "ti": Mock(try_number=2),
                    }
                ),
            },
        ),
    ):
        extract()

    assert captured["run_id"] == "scheduled__1.2"
    assert captured["token"] == "tok"
    # Catalog and schema separately: the landing is `raw.hubspot`, and a single dotted value
    # would be rejected by the loader's identifier validation.
    assert captured["catalog"] == "raw"
    assert captured["schema"] == "hubspot"
    assert captured["dry_run"] is False
    connection.close.assert_called_once()
    session.close.assert_called_once()


def test_a_dry_run_opens_no_connection():
    """The point of a dry run is to see the sweep's counts without a warehouse write."""
    extract = _callable("extract")
    globals_ = extract.__globals__

    connect = MagicMock()
    session = MagicMock()

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": connect,
                "make_session": MagicMock(return_value=session),
                "extract_office_holders": MagicMock(return_value={"dry_run": True}),
            },
        ),
        patch.dict(
            globals_,
            {
                "Variable": MagicMock(
                    get=Mock(
                        side_effect=lambda name, **kw: {
                            "hubspot_companion_loader_token": "tok",
                            "hubspot_companion_landing_schema": "raw.hubspot",
                        }[name]
                    )
                ),
                "get_current_context": Mock(
                    return_value={
                        "params": {"dry_run": True},
                        "dag_run": Mock(run_id="manual__1"),
                        "ti": Mock(try_number=1),
                    }
                ),
            },
        ),
    ):
        extract()

    connect.assert_not_called()
    session.close.assert_called_once()


def test_the_loader_reads_one_location_variable():
    """The companion tables live beside Lakeflow's in `raw`, so the loader cannot use the
    shared `databricks_catalog` Variable. One `catalog.schema` Variable rather than a pair,
    so an environment cannot be half-pointed at production, and so it matches the sibling
    companion DAG."""
    source = Path(_DAG_FILE).read_text()

    assert 'LANDING_SCHEMA_VARIABLE = "hubspot_companion_landing_schema"' in source
    assert 'Variable.get("databricks_catalog")' not in source
    assert "hubspot_companion_catalog" not in source
    assert 'hubspot_companion_schema"' not in source
