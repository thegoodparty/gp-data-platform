"""Structure assertions for the Bifrost loader DAG.

Loaded from the file path directly rather than the configured dags_folder, matching
test_gold_match_daily_dag.py: CI does not point dags_folder at astro/dags, and building the
DagBag at collection time keeps this on real Airflow with no metastore dependency.
"""

import logging
from contextlib import contextmanager
from pathlib import Path

import pytest

# The Astro runtime and the airflow uv env both have airflow; the stubbed collection in
# test_bifrost_export.py does not, so skip there rather than fail on the import.
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


def test_dag_loads():
    assert _DAG is not None, f"load_bifrost_usage failed to load from {_DAG_FILE}"


def test_serialised_and_paused_on_creation():
    """`max_active_runs=1` because two runs loading the same day would race on the anti-join,
    and every other DAG here sets it. `is_paused_upon_creation=True` because BUILD must not
    schedule anything: the export does not exist yet, so an unpaused DAG would just fail
    nightly until ops delivers."""
    assert _DAG.max_active_runs == 1
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_runs_after_the_export_and_clear_of_the_dbt_build():
    """The export lands about 09:00 UTC; this follows at 10:30. The 00:02 UTC dbt build is
    4pm Pacific, which is the gateway's busiest hour, so neither job goes near it."""
    assert _DAG.timetable.expression == "30 10 * * *"


def test_landing_table_is_created_before_the_first_insert():
    """The DAG owns the table, not a migration: it is created by the principal that writes it,
    with CREATE TABLE IF NOT EXISTS, so a hand-created table is a no-op rather than an error.
    If this ordering inverted, the first insert would fail on a missing relation."""
    load_day = _DAG.get_task("load_day")
    source = Path(load_day.python_callable.__code__.co_filename).read_text()

    create_at = source.index("ensure_landing_table(")
    insert_at = source.index("insert_day(")
    assert create_at < insert_at


def test_nothing_here_deletes_rows():
    """The landing table is append-only. Deleting an old observation would make the mart's
    history for that day diverge the next time it rebuilds, so growth is controlled by the
    insert's anti-join instead of by a prune."""
    source = Path(_DAG_FILE).read_text()
    lowered = source.lower()
    assert "delete from" not in lowered
    assert "drop table" not in lowered
    assert "merge into" not in lowered


def test_every_day_in_the_window_is_loaded_with_no_watermark():
    """The regression guard for the plan's critical defect: a "newest landed" watermark skips a
    re-emitted day, so a request first seen `processing` is never re-read as its terminal
    self. The only filter is the window."""
    source = Path(_DAG.get_task("list_days").python_callable.__code__.co_filename).read_text()
    # The module docstring explains why there is no watermark, so this looks at code only.
    code = source.split('"""', 2)[2]
    assert "days_in_window(" in code
    assert "max_loaded" not in code.lower()
    assert "watermark" not in code.lower()
