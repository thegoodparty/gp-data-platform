from __future__ import annotations

from hsmerge.types import Outcome, Pair, Status


def test_status_values_are_the_ones_the_log_records() -> None:
    """Catches: a rename here landing as a status dbt's queue does not recognise, so a
    merged pair stays eligible and merges twice."""
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


def test_a_pair_carries_the_two_contacts_and_optional_audit_fields() -> None:
    """Catches: audit fields becoming required, which hand-built pair lists cannot supply."""
    pair = Pair(primary_id="1", secondary_id="2")
    assert pair.gp_person_id is None
    assert pair.match_evidence is None
    assert Pair("1", "2", gp_person_id="p", match_evidence="same_phone").match_evidence == "same_phone"


def test_an_outcome_defaults_to_no_survivor_and_no_error() -> None:
    """Catches: a status that has neither field being forced to invent one."""
    outcome = Outcome(Status.SECONDARY_MISSING)
    assert outcome.survivor_id is None
    assert outcome.error is None
