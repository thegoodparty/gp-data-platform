"""Classification against the response bodies recorded in `reverse-etl/probes/README.md`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hsmerge.classify import Read, classify_merge, classify_read
from hsmerge.types import Pair, Status

PRIMARY = "260551983003"
SECONDARY = "260561373480"
THIRD = "260555246810"
ABSENT = "999999999999"

PAIR = Pair(primary_id=PRIMARY, secondary_id=SECONDARY)


@dataclass(frozen=True)
class FakeResponse:
    status_code: int
    body: dict[str, Any]


def _record(contact_id: str, **properties: Any) -> dict[str, Any]:
    """One result of a batch read, with the fields the endpoint returns around properties."""
    props = {
        "createdate": "2026-11-05T04:34:54.390Z",
        "email": f"probe-{contact_id}@example.com",
        "gp_person_id": None,
        "hs_all_contact_vids": contact_id,
        "hs_calculated_merged_vids": None,
        "hs_merged_object_ids": None,
        "hs_object_id": contact_id,
        "lastmodifieddate": "2026-11-05T04:34:54.390Z",
    }
    props.update(properties)
    return {
        "id": contact_id,
        "properties": props,
        "createdAt": "2026-11-05T04:34:54.390Z",
        "updatedAt": "2026-11-05T04:34:54.390Z",
        "archived": False,
        "url": f"https://app.hubspot.com/contacts/51780263/record/0-1/{contact_id}",
    }


def _read_body(*records: dict[str, Any], missing: str | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "completedAt": "2026-11-05T04:34:56.163Z",
        "status": "COMPLETE",
        "startedAt": "2026-11-05T04:34:56.121Z",
        "results": list(records),
    }
    if missing:
        body["errors"] = [
            {
                "status": "error",
                "category": "OBJECT_NOT_FOUND",
                "message": "Could not get some CONTACT objects, they may be deleted or not exist.",
                "context": {"ids": [missing]},
            }
        ]
        body["numErrors"] = 1
    return body


def test_already_merged_when_the_secondary_reads_as_the_primary() -> None:
    # A read of both ids after the merge: one record, the survivor naming both ids.
    read = Read.from_body(
        _read_body(
            _record(
                PRIMARY,
                hs_all_contact_vids=f"{PRIMARY};{SECONDARY}",
                hs_calculated_merged_vids=f"{SECONDARY}:1793853289193;{PRIMARY}:1793853289193",
                hs_merged_object_ids=f"{PRIMARY};{SECONDARY}",
            )
        )
    )

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.ALREADY_MERGED
    assert outcome.survivor_id == PRIMARY


def test_merged_elsewhere_when_the_secondary_reads_as_a_third_contact() -> None:
    # The secondary was absorbed by a contact outside the pair, whose record the read
    # returns in place of the id asked for.
    read = Read.from_body(
        _read_body(
            _record(THIRD, hs_merged_object_ids=f"{THIRD};{SECONDARY}"),
            _record(PRIMARY),
        )
    )

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.MERGED_ELSEWHERE
    assert outcome.survivor_id == THIRD


def test_secondary_missing_when_the_id_reads_as_nothing() -> None:
    read = Read.from_body(_read_body(_record(PRIMARY), missing=SECONDARY))

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.SECONDARY_MISSING


def test_primary_missing_when_the_primary_id_reads_as_nothing() -> None:
    read = Read.from_body(_read_body(_record(SECONDARY), missing=PRIMARY))

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.PRIMARY_MISSING


def test_primary_merged_away_when_the_primary_reads_as_another_contact() -> None:
    read = Read.from_body(_read_body(_record(THIRD, hs_merged_object_ids=f"{THIRD};{PRIMARY}")))

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.PRIMARY_MERGED_AWAY


def test_merged_when_the_read_allows_it_and_the_merge_returns_the_primary() -> None:
    # The only read that merges: two records, each its own, so the read settles nothing.
    assert classify_read(PAIR, Read.from_body(_read_body(_record(PRIMARY), _record(SECONDARY)))) is None

    response = FakeResponse(
        status_code=200,
        body={
            "id": PRIMARY,
            "createdAt": "2026-11-05T04:34:47.189Z",
            "updatedAt": "2026-11-05T04:34:49.193Z",
            "archived": False,
            "url": f"https://app.hubspot.com/contacts/51780263/record/0-1/{PRIMARY}",
            "properties": {"hs_merged_object_ids": f"{PRIMARY};{SECONDARY}"},
        },
    )

    outcome = classify_merge(PAIR, response)

    assert outcome.status is Status.MERGED
    assert outcome.survivor_id == PRIMARY


def test_id_changed_when_the_merge_returns_a_new_record() -> None:
    response = FakeResponse(status_code=200, body={"id": THIRD, "archived": False})

    outcome = classify_merge(PAIR, response)

    assert outcome.status is Status.ID_CHANGED
    assert outcome.survivor_id == THIRD


def test_failed_when_the_merge_is_refused() -> None:
    # The refusal a pair that read as mergeable can still earn, if it moved underneath.
    response = FakeResponse(
        status_code=400,
        body={
            "status": "error",
            "message": (
                "Cannot merge ObjectCoordinates{portalId=51780263, objectTypeId=ObjectTypeId"
                "{legacyObjectType=CONTACT}, objectId=260561373480} because it has a forward "
                "reference to 260551983003. Only canonical objects can be merged."
            ),
            "correlationId": "01a1a9fc-fa14-7d9b-a1aa-e86cf02431a4",
            "category": "VALIDATION_ERROR",
        },
    )

    outcome = classify_merge(PAIR, response)

    assert outcome.status is Status.FAILED
    assert outcome.error is not None
    assert outcome.error.startswith("400 VALIDATION_ERROR")
