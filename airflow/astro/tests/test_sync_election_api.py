"""Unit tests for the sync_election_api DAG row-transform helpers.

The transforms are pure tuple-position mappings, so a column-order mistake
silently corrupts Postgres data. These tests pin the field positions
explicitly. Mocks airflow/databricks/etc. so the file collects without the
Astro runtime installed.
"""

import sys
from datetime import datetime
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
    DTI_COLUMNS,
    EOS_COLUMNS,
    PT_COLUMNS,
    ZTP_SOURCE_COLUMNS,
    ZTP_TARGET_COLUMNS,
    _pt_quality_gate,
    _ztp_transform_row,
)


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
    # ZTP_SOURCE_COLUMNS pins the input order; the dict is just for
    # readability — build the tuple by indexing into it.
    row = tuple(source_values[c] for c in ZTP_SOURCE_COLUMNS)

    out = _ztp_transform_row(row)

    assert len(out) == len(ZTP_TARGET_COLUMNS)
    out_by_name = dict(zip(ZTP_TARGET_COLUMNS, out, strict=False))

    # Generated fields
    assert isinstance(out_by_name["id"], str) and len(out_by_name["id"]) == 36
    assert isinstance(out_by_name["updated_at"], datetime)

    # Pass-through fields land in the correct positions
    for col, expected in source_values.items():
        assert out_by_name[col] == expected, f"{col} did not pass through"


def test_ztp_transform_row_id_is_deterministic():
    """uuid5(zip_code|position_id|election_date) — same input, same id."""
    row = tuple(
        {
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
        }[c]
        for c in ZTP_SOURCE_COLUMNS
    )

    id_first = _ztp_transform_row(row)[0]
    id_second = _ztp_transform_row(row)[0]
    assert id_first == id_second


def test_ztp_source_columns_match_transform_arity():
    """Guards against ZTP_SOURCE_COLUMNS / _ztp_transform_row drift."""
    row = tuple(range(len(ZTP_SOURCE_COLUMNS)))
    out = _ztp_transform_row(row)
    assert len(out) == len(ZTP_TARGET_COLUMNS)


def test_dti_columns_pinned():
    """Pin DTI_COLUMNS to catch silent column reorderings.

    The DistrictTopIssue bulk-insert path passes values to
    psycopg2.extras.execute_values positionally, so swapping or dropping a
    DTI_COLUMNS entry would land the wrong value into Postgres without any
    error at insert time. Pin the list so reorderings show up as a failing
    test instead of a corrupted DistrictTopIssue table.
    """
    assert DTI_COLUMNS == [
        "id",
        "updated_at",
        "district_id",
        "issue",
        "issue_label",
        "score",
        "is_local",
        "is_regional",
        "is_state",
        "is_federal",
        "issue_rank",
    ]


def test_eos_columns_pinned():
    """Pin EOS_COLUMNS to catch silent column reorderings.

    Elected_Office_Support loads with no row transform: EOS_COLUMNS drives both
    the Databricks SELECT order and the positional Postgres insert, so a
    reordering would land values in the wrong columns without an insert-time
    error. The mart's column order must match this list.
    """
    assert EOS_COLUMNS == [
        "elected_office_id",
        "support_constituents",
        "total_constituents",
        "created_at",
        "updated_at",
    ]


def test_pt_columns_pinned():
    """Pin PT_COLUMNS to catch silent column reorderings.

    Projected_Turnout loads with no row transform: PT_COLUMNS drives both the
    Databricks SELECT order and the positional Postgres insert. id/district_id
    are both uuids and created_at/updated_at/inference_at are all timestamps,
    so a swap within either group would corrupt data without any insert-time
    error. The order mirrors the legacy dbt writer's column mapping for this
    table.
    """
    assert PT_COLUMNS == [
        "id",
        "created_at",
        "updated_at",
        "election_year",
        "election_code",
        "projected_turnout",
        "inference_at",
        "model_version",
        "district_id",
    ]


def test_pt_quality_gate_refuses_duplicate_keys():
    """Any duplicate (district_id, election_year, election_code) key refuses
    the swap — the invariant the swap delivery exists to guarantee."""
    with pytest.raises(ValueError, match="duplicate"):
        _pt_quality_gate(loaded_count=800_000, dup_keys=1, prior_key_count=800_000)


def test_pt_quality_gate_refuses_coverage_collapse():
    """Staging under half the prior distinct-key count refuses the swap."""
    with pytest.raises(ValueError, match="refusing to swap"):
        _pt_quality_gate(loaded_count=300_000, dup_keys=0, prior_key_count=800_000)


def test_pt_quality_gate_allows_dedupe_cutover():
    """Keys-vs-keys baseline: a deduped load matching the prior key count
    passes even when the prior table's RAW row count was much larger (bloat
    from the legacy upsert path must not refuse a legitimate cutover)."""
    _pt_quality_gate(loaded_count=800_000, dup_keys=0, prior_key_count=800_000)


def test_pt_quality_gate_boundary_ratio_passes():
    """Exactly 0.5 passes — same boundary semantics as the other groups."""
    _pt_quality_gate(loaded_count=400_000, dup_keys=0, prior_key_count=800_000)


def test_pt_quality_gate_cold_start_floor():
    """No prior table: implausibly small loads refuse, plausible loads pass."""
    with pytest.raises(ValueError, match="Cold-start"):
        _pt_quality_gate(loaded_count=99_999, dup_keys=0, prior_key_count=0)
    _pt_quality_gate(loaded_count=100_000, dup_keys=0, prior_key_count=0)
