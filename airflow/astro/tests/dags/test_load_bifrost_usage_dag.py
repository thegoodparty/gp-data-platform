"""Behavioural assertions for the Bifrost loader DAG.

Loaded from the file path directly rather than the configured dags_folder, matching
test_gold_match_daily_dag.py: CI does not point dags_folder at astro/dags, and building the
DagBag at collection time keeps this on real Airflow with no metastore dependency.

These call the task callables rather than reading the DAG file as text. A source-grep test
passes just as happily against a DAG that does nothing, which is the failure mode worth
avoiding in the one test that is supposed to prove the loader behaves.
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    """`max_active_runs=1` because two runs loading the same day would race on the anti-join.
    `is_paused_upon_creation=True` because BUILD must not schedule anything: the export does not
    exist yet, so an unpaused DAG would just fail nightly until ops delivers."""
    assert _DAG.max_active_runs == 1
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_runs_after_the_export_and_clear_of_the_dbt_build():
    """The export lands about 09:00 UTC; this follows at 10:30. The 00:02 UTC dbt build is 4pm
    Pacific, the gateway's busiest hour, so neither job goes near it."""
    assert _DAG.timetable.expression == "30 10 * * *"


def test_mapped_loads_are_serialised():
    """Every instance appends to the same unpartitioned Delta table and reads it to decide what
    changed; in parallel those read-modify-write cycles can raise ConcurrentAppendException."""
    assert _DAG.get_task("load_day").max_active_tis_per_dagrun == 1


def test_a_failed_load_fails_the_run():
    """`summarize` is the only leaf. With `all_done` it would end the run green after a failed
    load, and that day's spend would be missing with nothing red to show for it."""
    assert _DAG.get_task("summarize").trigger_rule == "all_success"


def test_load_day_creates_the_table_before_it_inserts():
    """The DAG owns the landing table. If this ordering inverted, the first insert would fail on
    a missing relation."""
    calls = []
    connection = MagicMock()
    connection.__enter__ = MagicMock(return_value=connection)
    connection.__exit__ = MagicMock(return_value=False)

    load_day = _callable("load_day")
    globals_ = load_day.__globals__

    def record_ensure(*args, **kwargs):
        calls.append("ensure")

    def record_insert(*args, **kwargs):
        calls.append("insert")
        return 3

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": MagicMock(return_value=connection),
                "ensure_landing_table": record_ensure,
                "insert_day": record_insert,
            },
        ),
        patch.object(globals_["Variable"], "get", side_effect=lambda name, **kw: f"v-{name}"),
    ):
        inserted = load_day({"day": "2026-11-04", "bucket": "b", "prefix": "p"})

    assert calls == ["ensure", "insert"]
    assert inserted == 3


def test_load_day_resolves_the_databricks_connection_by_variable_name():
    """`connect_from_conn_id` does its own Variable lookup; passing it a resolved value makes it
    treat a connection id as another Variable name and fails before the DDL."""
    connection = MagicMock()
    connection.__enter__ = MagicMock(return_value=connection)
    connection.__exit__ = MagicMock(return_value=False)

    load_day = _callable("load_day")
    globals_ = load_day.__globals__
    connect = MagicMock(return_value=connection)

    with (
        patch.dict(
            globals_,
            {
                "connect_from_conn_id": connect,
                "ensure_landing_table": MagicMock(),
                "insert_day": MagicMock(return_value=0),
            },
        ),
        patch.object(globals_["Variable"], "get", return_value="x"),
    ):
        load_day({"day": "2026-11-04", "bucket": "b", "prefix": "p"})

    # `connect_from_conn_id` resolves the Variable itself; handing it a resolved value makes it
    # look up a connection id as if it were a Variable name and fail before the DDL.
    connect.assert_called_once_with()


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_a_missing_marker_skips_the_day(code):
    from botocore.exceptions import ClientError

    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "bifrost/logs/dt=2026-11-04/"}]}
    ]
    s3.head_object.side_effect = ClientError({"Error": {"Code": code}}, "HeadObject")

    assert _callable("list_days").__globals__["discover_complete_days"](s3, "bucket", "bifrost/logs") == []


@pytest.mark.parametrize("code", ["403", "AccessDenied", "SlowDown", "500"])
def test_a_marker_read_that_failed_for_another_reason_raises(code):
    """Treating these as "not complete" would skip every day, land nothing, and end green: the
    exact silent understatement this loader exists to avoid."""
    from botocore.exceptions import ClientError

    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "bifrost/logs/dt=2026-11-04/"}]}
    ]
    s3.head_object.side_effect = ClientError({"Error": {"Code": code}}, "HeadObject")

    with pytest.raises(ClientError):
        _callable("list_days").__globals__["discover_complete_days"](s3, "bucket", "bifrost/logs")


def test_a_marked_day_is_returned():
    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [
        {"CommonPrefixes": [{"Prefix": "bifrost/logs/dt=2026-11-04/"}]},
        {"CommonPrefixes": [{"Prefix": "bifrost/logs/dt=2026-11-03/"}]},
    ]
    s3.head_object.return_value = {}

    assert _callable("list_days").__globals__["discover_complete_days"](s3, "bucket", "bifrost/logs") == [
        ("2026-11-03", "bifrost/logs/dt=2026-11-03/"),
        ("2026-11-04", "bifrost/logs/dt=2026-11-04/"),
    ]
