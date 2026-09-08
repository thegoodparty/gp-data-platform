from __future__ import annotations

from datetime import date, datetime

from retl.payload import build_payload, serialize_payload


def test_build_payload_omits_null_values() -> None:
    """Catches: a None column value getting sent as JSON null, which HubSpot would clear."""
    payload = build_payload(
        {"gp_person_id": "p1", "firstname": "Jane", "phone": None}, excluded_columns=frozenset()
    )
    assert payload == {"gp_person_id": "p1", "firstname": "Jane"}


def test_build_payload_omits_empty_string_values() -> None:
    """Catches: an empty string reaching the payload, which HubSpot's API reads as 'clear this property'."""
    payload = build_payload({"gp_person_id": "p1", "phone": ""}, excluded_columns=frozenset())
    assert payload == {"gp_person_id": "p1"}


def test_build_payload_drops_excluded_columns() -> None:
    """Catches: a sales-owned or build-clock column leaking into the payload despite being excluded."""
    payload = build_payload(
        {"gp_person_id": "p1", "firstname": "Jane", "added_to_mart_at": "2026-01-01"},
        excluded_columns=frozenset({"added_to_mart_at"}),
    )
    assert payload == {"gp_person_id": "p1", "firstname": "Jane"}


def test_build_payload_keeps_key_column_as_a_property() -> None:
    """Catches: the key column being silently dropped from the payload instead of just used for keying.

    gp_person_id must be written as a real HubSpot property -- it is a unique
    property on the contact, not only a diff key -- so it is not in
    excluded_columns and must survive into the payload.
    """
    payload = build_payload({"gp_person_id": "p1", "firstname": "Jane"}, excluded_columns=frozenset())
    assert payload["gp_person_id"] == "p1"


def test_build_payload_normalizes_dates_and_decimals_to_strings() -> None:
    """Catches: a raw date/datetime object reaching json.dumps and raising, instead of a stable string."""
    payload = build_payload(
        {"gp_person_id": "p1", "election_date": date(2026, 11, 3), "sent_at": datetime(2026, 1, 1, 12, 0, 0)},
        excluded_columns=frozenset(),
    )
    assert payload == {
        "gp_person_id": "p1",
        "election_date": "2026-11-03",
        "sent_at": "2026-01-01T12:00:00",
    }


def test_build_payload_preserves_falsy_non_blank_values() -> None:
    """Catches: 0, 0.0, and False being mistaken for blanks and dropped alongside None and ''."""
    payload = build_payload(
        {"gp_person_id": "p1", "viability_score": 0, "is_winner": False},
        excluded_columns=frozenset(),
    )
    assert payload == {"gp_person_id": "p1", "viability_score": 0, "is_winner": False}


def test_serialize_payload_sorts_keys() -> None:
    """Catches: payload equality depending on column order, which would make a byte-identical
    comparison against sent_log flap based on incidental SELECT * column ordering."""
    assert serialize_payload({"b": "2", "a": "1"}) == serialize_payload({"a": "1", "b": "2"})


def test_serialize_payload_is_byte_identical_on_unchanged_input() -> None:
    """Catches: any nondeterminism in serialization, which would look like a changed payload
    (and cause a mass resend) even though nothing about the source data changed."""
    payload: dict[str, str | int] = {"gp_person_id": "p1", "firstname": "Jane", "viability_score": 42}
    assert serialize_payload(payload) == serialize_payload(dict(payload))
