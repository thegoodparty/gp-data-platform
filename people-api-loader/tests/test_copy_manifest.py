"""CopyTableResult carries a per-unit discriminator: `table` is the real table name
(Voter, District, DistrictStats, DistrictVoter), and `state` is the state for a
partitioned table or "" for a flat table's single whole-table unit."""

from __future__ import annotations

from loader.people_api.manifests import CopyTableResult


def test_copy_table_result_has_state() -> None:
    r = CopyTableResult(
        table="Voter",
        state="TX",
        expected_rows=100,
        actual_rows=100,
        files_loaded=3,
        seconds_elapsed=1.5,
    )
    assert r.state == "TX"
    assert r.table == "Voter"


def test_copy_table_result_flat_table_uses_empty_state() -> None:
    # Flat (non-partitioned) tables load as a single whole-table unit, keyed by
    # state="" — mirrors UnloadFile/UnloadTable's convention for flat tables.
    r = CopyTableResult(
        table="District",
        state="",
        expected_rows=50,
        actual_rows=50,
        files_loaded=1,
        seconds_elapsed=0.5,
    )
    assert r.table == "District"
    assert r.state == ""
