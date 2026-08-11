"""Unit tests for the sync_election_api DAG declarations and pure helpers.

The row transforms are pure tuple-position mappings, so an index mistake
silently corrupts Postgres data — those positions are pinned here. The
declaration-consistency test guards the cross-group FK wiring: a MartSync
whose fkeys and parents disagree would wedge a parent's swap in production.
Mocks airflow/databricks/etc. so the file collects without the Astro runtime
installed.
"""

import sys
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import pytest

# Stub external modules so the DAG file can be imported in any environment.
_STUBS = (
    "airflow",
    "airflow.decorators",
    "airflow.sdk",
    "databricks",
    "databricks.sql",
    "databricks.sql.client",
    "databricks.sdk",
    "databricks.sdk.core",
    "paramiko",
    "pendulum",
    "sshtunnel",
    "psycopg2",
    "psycopg2.extras",
)
for _mod in _STUBS:
    sys.modules[_mod] = MagicMock()

from dags.sync_election_api import (  # noqa: E402
    TABLES,
    ZTP_SOURCE_COLUMNS,
    ZTP_TARGET_COLUMNS,
    _position_transform_row,
    _prepend_timestamps,
    _swap_enabled,
    _ztp_transform_row,
    check_race_window,
)

# ---------------------------------------------------------------------------
# Declaration consistency
# ---------------------------------------------------------------------------


def test_parents_match_fkey_references():
    """Every non-self FK must have its referenced table's group in `parents`
    (and vice versa): a missing edge lets a parent swap wedge on the child's
    staging FK; a stale edge points at a group that no longer exists."""
    table_to_group = {t.spec.target_table: t.group_id for t in TABLES}
    for t in TABLES:
        referenced_groups = {
            table_to_group[fk.ref_table] for fk in t.spec.fkeys if fk.ref_table != t.spec.target_table
        }
        assert referenced_groups == set(t.parents), t.group_id


def test_inbound_fkeys_declared_on_both_sides():
    """A child's outbound FK to a swapped parent must appear in that parent's
    inbound_fkeys (same constraint name), or the parent's swap will wedge on
    the live child's constraint."""
    by_table = {t.spec.target_table: t for t in TABLES}
    for child in TABLES:
        for fk in child.spec.fkeys:
            if fk.ref_table == child.spec.target_table:
                continue  # self-refs ride along with the table's own renames
            parent = by_table[fk.ref_table]
            assert fk.name in {
                ifk.constraint_name for ifk in parent.spec.inbound_fkeys
            }, f"{parent.group_id} must re-point {fk.name}"


def test_transform_target_alignment():
    """A transform that changes row arity must come with target_columns of
    the matching length (positional inserts corrupt silently otherwise)."""
    for t in TABLES:
        if t.transform_row is None:
            assert t.target_columns == () or len(t.target_columns) == len(t.source_columns)
            continue
        probe = tuple(range(len(t.source_columns)))
        assert len(t.transform_row(probe)) == len(t.insert_columns), t.group_id


# ---------------------------------------------------------------------------
# Row transforms
# ---------------------------------------------------------------------------


def test_ztp_transform_row_field_positions():
    """Each input field lands at its expected index in the output tuple."""
    source_values = {
        "position_id": "pos-1",
        "name": "Mayor",
        "br_database_id": 12345,
        "zip_code": "90210",
        "election_year": 2026,
        "election_date": "2026-11-03",
        "display_office_level": "City",
        "office_type": "Mayor",
        "state": "CA",
        "district": None,
        "voters_in_zip": 15688,
        "voters_in_zip_district": 9714,
        "pct_districtzip_to_zip": 0.619,
    }
    row = tuple(source_values[c] for c in ZTP_SOURCE_COLUMNS)

    out = _ztp_transform_row(row)

    assert len(out) == len(ZTP_TARGET_COLUMNS)
    out_by_name = dict(zip(ZTP_TARGET_COLUMNS, out, strict=True))

    # Generated fields
    assert isinstance(out_by_name["id"], str) and len(out_by_name["id"]) == 36
    assert isinstance(out_by_name["updated_at"], datetime)

    # Pass-through fields land in the correct positions
    for col, expected in source_values.items():
        assert out_by_name[col] == expected, f"{col} did not pass through"

    # uuid5 of the natural key: same input, same id
    assert _ztp_transform_row(row)[0] == out[0]


def test_prepend_timestamps_keeps_id_first():
    out = _prepend_timestamps(("some-id", "a", "b"))
    assert out[0] == "some-id"
    assert isinstance(out[1], datetime) and isinstance(out[2], datetime)
    assert out[3:] == ("a", "b")


def test_position_transform_stringifies_br_database_id():
    """Position.br_database_id is bigint in the mart but text in Postgres;
    psycopg2 would ship an int literal that Postgres refuses to assign."""
    out = _position_transform_row(("id-1", 8437291, "br-pos", "CA"))
    assert out == ("id-1", "8437291", "br-pos", "CA")


# ---------------------------------------------------------------------------
# Pure gates
# ---------------------------------------------------------------------------

RACE_TODAY = date(2026, 7, 23)


def test_race_window_accepts_in_window_dates():
    check_race_window(
        RACE_TODAY - timedelta(days=700),
        RACE_TODAY + timedelta(days=700),
        RACE_TODAY,
    )


def test_race_window_refuses_out_of_window_and_empty():
    with pytest.raises(ValueError, match="outside window"):
        check_race_window(RACE_TODAY - timedelta(days=2500), RACE_TODAY, RACE_TODAY)
    with pytest.raises(ValueError, match="outside window"):
        check_race_window(RACE_TODAY, RACE_TODAY + timedelta(days=1200), RACE_TODAY)
    with pytest.raises(ValueError, match="empty"):
        check_race_window(None, None, RACE_TODAY)


def test_swap_enabled_parse():
    assert _swap_enabled("true")
    assert _swap_enabled("  TRUE  ")
    assert not _swap_enabled("false")
    assert not _swap_enabled("")
    assert not _swap_enabled("yes")
    assert not _swap_enabled("1")


def test_psycopg2_adapts_python_lists_to_postgres_arrays():
    """Array round-trips (Race frequency int[], Candidacy urls text[]): the
    arrow-backed connector returns numpy arrays, the loader normalizes them
    to Python lists, and those lists must adapt to ARRAY literals."""
    from psycopg2.extensions import adapt

    assert adapt([1, 2]).getquoted() == b"ARRAY[1,2]"
    assert adapt(["a", "b"]).getquoted() == b"ARRAY['a','b']"
