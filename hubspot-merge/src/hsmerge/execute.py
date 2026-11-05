"""Runs pairs through HubSpot: per chunk, one batch read, then classify, merge and log
each pair.

There is no resume logic. A rerun reads the queue, which drops logged pairs, and a pair
merged but not logged reads as `already_merged`.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from itertools import batched
from typing import Protocol

from .classify import classify_merge, classify_read
from .client import ReadResult, Record
from .types import LogEntry, Outcome, Pair, Status

# 50 pairs is 100 ids, the batch read's limit.
CHUNK_SIZE = 50

# Kept in each log snapshot, so a wrong merge can be rebuilt by hand.
SNAPSHOT_PROPERTIES = (
    "email",
    "hs_additional_emails",
    "firstname",
    "lastname",
    "phone",
    "mobilephone",
    "hubspot_owner_id",
    "lifecyclestage",
    "gp_person_id",
    "br_candidacy_id",
    "createdate",
    "lastmodifieddate",
)


class HubSpot(Protocol):
    def batch_read(self, ids: Sequence[str], properties: Sequence[str]) -> ReadResult: ...

    def merge(self, primary_id: str, secondary_id: str) -> Record: ...


class Log(Protocol):
    def write(self, entries: Sequence[LogEntry]) -> None: ...


def preview(pairs: Iterable[Pair], hubspot: HubSpot) -> Iterator[tuple[Pair, Outcome | None]]:
    """What the read settles for each pair, or None for a pair that would merge."""
    for chunk, read in _read_chunks(pairs, hubspot):
        for pair in chunk:
            yield pair, classify_read(pair, read)


def execute(pairs: Iterable[Pair], hubspot: HubSpot, log: Log) -> Counter[Status]:
    """Merge every pair the read allows, and log one row per pair. Returns the status counts."""
    counts: Counter[Status] = Counter()
    for chunk, read in _read_chunks(pairs, hubspot):
        entries = [_attempt(pair, read, hubspot) for pair in chunk]
        log.write(entries)
        counts.update(entry.outcome.status for entry in entries)
    return counts


def verify(pairs: Iterable[Pair], hubspot: HubSpot) -> Iterator[tuple[Pair, str]]:
    """Each pair logged as `merged` that HubSpot does not show as merged, with the reason."""
    for chunk, read in _read_chunks(pairs, hubspot):
        for pair in chunk:
            primary = read.record_for(pair.primary_id)
            secondary = read.record_for(pair.secondary_id)
            if primary is None or primary.id != pair.primary_id:
                yield pair, "the primary no longer reads as itself"
            elif secondary is None or secondary.id != pair.primary_id:
                yield pair, "the secondary does not resolve to the primary"


def _read_chunks(pairs: Iterable[Pair], hubspot: HubSpot) -> Iterator[tuple[Sequence[Pair], ReadResult]]:
    for chunk in batched(pairs, CHUNK_SIZE):
        ids = [contact_id for pair in chunk for contact_id in (pair.primary_id, pair.secondary_id)]
        yield chunk, hubspot.batch_read(ids, SNAPSHOT_PROPERTIES)


def _attempt(pair: Pair, read: ReadResult, hubspot: HubSpot) -> LogEntry:
    primary = read.record_for(pair.primary_id)
    secondary = read.record_for(pair.secondary_id)
    outcome = classify_read(pair, read) or _merge(pair, hubspot)
    return LogEntry(
        pair,
        outcome,
        primary_snapshot=primary.properties if primary else None,
        secondary_snapshot=secondary.properties if secondary else None,
    )


def _merge(pair: Pair, hubspot: HubSpot) -> Outcome:
    try:
        survivor = hubspot.merge(pair.primary_id, pair.secondary_id)
    except Exception as error:
        # A refusal, retries run out, or a timeout. Log it and go on: the pair stays in
        # the queue, and the next run's read settles it.
        return Outcome(Status.FAILED, error=f"{type(error).__name__}: {error}")
    return classify_merge(pair, survivor)
