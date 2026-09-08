from __future__ import annotations

from retl.diff import compute_to_send, orphaned_keys


def test_compute_to_send_includes_a_key_with_no_logged_payload() -> None:
    """Catches: a brand-new person never being offered to the destination."""
    to_send = compute_to_send({"p1": '{"firstname":"Jane"}'}, {})
    assert to_send == {"p1": '{"firstname":"Jane"}'}


def test_compute_to_send_excludes_a_key_whose_payload_is_unchanged() -> None:
    """Catches: steady-state days re-sending everyone instead of costing nothing."""
    to_send = compute_to_send({"p1": '{"firstname":"Jane"}'}, {"p1": '{"firstname":"Jane"}'})
    assert to_send == {}


def test_compute_to_send_includes_a_key_whose_payload_changed() -> None:
    """Catches: a real update (e.g. a recomputed viability score) failing to resend."""
    to_send = compute_to_send({"p1": '{"score":4}'}, {"p1": '{"score":3}'})
    assert to_send == {"p1": '{"score":4}'}


def test_compute_to_send_resends_a_value_that_returned_to_a_previous_state() -> None:
    """Catches: subtracting ALL history instead of the latest row per key.

    A score that goes 3 -> 4 -> 3 must resend on the second 3, or the destination
    is stuck showing 4 forever. Here `latest_sent` holds only the most recent
    logged payload (4), so today's 3 must be offered again.
    """
    to_send = compute_to_send({"p1": '{"score":3}'}, {"p1": '{"score":4}'})
    assert to_send == {"p1": '{"score":3}'}


def test_orphaned_keys_finds_a_logged_key_missing_from_the_current_population() -> None:
    """Catches: a person-id remint going unnoticed (the old id stays in sent_log forever
    once the model stops emitting it)."""
    assert orphaned_keys({"old_id": "{}"}, {"new_id": "{}"}) == {"old_id"}


def test_orphaned_keys_is_empty_when_every_logged_key_still_appears() -> None:
    """Catches: a false-positive orphan count on an ordinary day where nobody was remitted."""
    assert orphaned_keys({"p1": "{}"}, {"p1": "{}", "p2": "{}"}) == set()
