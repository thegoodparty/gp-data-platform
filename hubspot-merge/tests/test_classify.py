"""What a status means, over trimmed dicts shaped like the sandbox's recorded answers.

The parse rule is tested once, in `test_client.py`; this file builds records through the
client's own parser and asks only what a status means.
"""

from __future__ import annotations

from hsmerge.classify import classify_merge, classify_read
from hsmerge.client import ReadResult
from hsmerge.client import _record as parse_record
from hsmerge.types import Pair, Status
from tests._fakes import LIVE_RECORD, MERGED_RECORD, PRE_MERGE_RECORD, SURVIVOR_RECORD

PRIMARY = SURVIVOR_RECORD["id"]
SECONDARY = LIVE_RECORD["id"]
THIRD = "260555246810"

PAIR = Pair(primary_id=PRIMARY, secondary_id=SECONDARY)


def _read(*records: dict, missing: str | None = None) -> ReadResult:
    return ReadResult(
        records={payload["id"]: parse_record(payload) for payload in records},
        missing_ids=(missing,) if missing else (),
    )


def test_already_merged_when_the_secondary_reads_as_the_primary() -> None:
    # A read of both ids after the merge: one record, the survivor naming both ids.
    outcome = classify_read(PAIR, _read(SURVIVOR_RECORD))

    assert outcome is not None
    assert outcome.status is Status.ALREADY_MERGED
    assert outcome.survivor_id == PRIMARY


def test_merged_elsewhere_when_the_secondary_reads_as_a_third_contact() -> None:
    # The secondary was absorbed by a contact outside the pair, whose record the read
    # returns in place of the id asked for.
    read = _read(
        {"id": THIRD, "properties": {"hs_merged_object_ids": f"{THIRD};{SECONDARY}"}},
        {"id": PRIMARY, "properties": {}},
    )

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.MERGED_ELSEWHERE
    assert outcome.survivor_id == THIRD


def test_secondary_missing_when_the_id_reads_as_nothing() -> None:
    outcome = classify_read(PAIR, _read({"id": PRIMARY, "properties": {}}, missing=SECONDARY))

    assert outcome is not None
    assert outcome.status is Status.SECONDARY_MISSING


def test_primary_missing_when_the_primary_id_reads_as_nothing() -> None:
    outcome = classify_read(PAIR, _read({"id": SECONDARY, "properties": {}}, missing=PRIMARY))

    assert outcome is not None
    assert outcome.status is Status.PRIMARY_MISSING


def test_primary_merged_away_when_the_primary_reads_as_another_contact() -> None:
    read = _read({"id": THIRD, "properties": {"hs_merged_object_ids": f"{THIRD};{PRIMARY}"}})

    outcome = classify_read(PAIR, read)

    assert outcome is not None
    assert outcome.status is Status.PRIMARY_MERGED_AWAY


def test_merged_when_the_read_allows_it_and_the_merge_returns_the_primary() -> None:
    # The only read that merges: two live records, each speaking only for itself, so the
    # read settles nothing. Both orders, since attribution must not depend on the order the
    # records come back in.
    assert classify_read(PAIR, _read(PRE_MERGE_RECORD, LIVE_RECORD)) is None
    assert classify_read(PAIR, _read(LIVE_RECORD, PRE_MERGE_RECORD)) is None

    outcome = classify_merge(PAIR, parse_record(MERGED_RECORD))

    assert outcome.status is Status.MERGED
    assert outcome.survivor_id == PRIMARY


def test_id_changed_when_the_merge_returns_a_new_record() -> None:
    outcome = classify_merge(PAIR, parse_record({**MERGED_RECORD, "id": THIRD}))

    assert outcome.status is Status.ID_CHANGED
    assert outcome.survivor_id == THIRD
