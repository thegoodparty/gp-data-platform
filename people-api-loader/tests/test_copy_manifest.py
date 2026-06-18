"""CopyTableResult carries a per-state discriminator (table is always 'Voter')."""

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
