"""Unit tests for the race swap's task-level quality gate (pure function)."""

from datetime import date, timedelta

import pytest
from dags.sync_election_api import RACE_COLUMNS, _race_quality_gate

TODAY = date(2026, 7, 23)
IN_WINDOW_MIN = TODAY - timedelta(days=700)
IN_WINDOW_MAX = TODAY + timedelta(days=700)


def _ok(**overrides):
    kwargs = dict(
        loaded_count=400_000,
        dup_ids=0,
        null_ids=0,
        prior_count=390_000,
        min_election_date=IN_WINDOW_MIN,
        max_election_date=IN_WINDOW_MAX,
        today=TODAY,
        unknown_live_columns=set(),
        missing_live_columns=set(),
    )
    kwargs.update(overrides)
    return kwargs


def test_healthy_load_passes():
    _race_quality_gate(**_ok())


def test_duplicate_ids_refuse():
    with pytest.raises(ValueError, match="duplicate"):
        _race_quality_gate(**_ok(dup_ids=1))


def test_null_ids_refuse():
    with pytest.raises(ValueError, match="NULL id"):
        _race_quality_gate(**_ok(null_ids=1))


def test_count_collapse_refuses():
    with pytest.raises(ValueError, match="ratio"):
        _race_quality_gate(**_ok(loaded_count=100_000))


def test_unknown_live_column_refuses():
    with pytest.raises(ValueError, match="does not supply"):
        _race_quality_gate(**_ok(unknown_live_columns={"projected_turnout"}))


def test_missing_live_column_refuses():
    with pytest.raises(ValueError, match="live Race lacks"):
        _race_quality_gate(**_ok(missing_live_columns={"office_level"}))


def test_out_of_window_dates_refuse():
    with pytest.raises(ValueError, match="outside window"):
        _race_quality_gate(**_ok(min_election_date=TODAY - timedelta(days=2500)))
    with pytest.raises(ValueError, match="outside window"):
        _race_quality_gate(**_ok(max_election_date=TODAY + timedelta(days=1200)))


def test_empty_staging_refuses():
    with pytest.raises(ValueError, match="empty"):
        _race_quality_gate(**_ok(min_election_date=None, max_election_date=None))


def test_cold_start_floor():
    with pytest.raises(ValueError, match="implausibly small"):
        _race_quality_gate(**_ok(prior_count=0, loaded_count=50_000))


def test_race_columns_match_prisma_shape():
    # 36 columns, id first; frequency and position_names are the array pair
    # whose round-trip the loader comment documents.
    assert len(RACE_COLUMNS) == 36
    assert RACE_COLUMNS[0] == "id"
    assert {"frequency", "position_names"} <= set(RACE_COLUMNS)


def test_psycopg2_adapts_python_lists_to_postgres_arrays():
    """The race loader is the framework's first array round-trip
    (frequency int[], position_names text[]): rows arrive from the
    Databricks connector as Python lists and must adapt to ARRAY literals."""
    from psycopg2.extensions import adapt

    assert adapt([1, 2]).getquoted() == b"ARRAY[1,2]"
    assert adapt(["a", "b"]).getquoted() == b"ARRAY['a','b']"


def test_race_swap_enabled_parse():
    from dags.sync_election_api import _race_swap_enabled

    assert _race_swap_enabled("true")
    assert _race_swap_enabled("TRUE")
    assert _race_swap_enabled("  true  ")
    assert not _race_swap_enabled("false")
    assert not _race_swap_enabled("")
    assert not _race_swap_enabled("yes")
    assert not _race_swap_enabled("1")
