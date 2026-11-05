from __future__ import annotations

from hsmerge.types import Status


def test_status_values_are_the_ones_the_log_records() -> None:
    """A rename here would land as a status dbt's queue does not exclude, so the pair
    would stay eligible and merge twice."""
    assert {status.value for status in Status} == {
        "merged",
        "already_merged",
        "merged_elsewhere",
        "secondary_missing",
        "primary_merged_away",
        "primary_missing",
        "id_changed",
        "failed",
    }
