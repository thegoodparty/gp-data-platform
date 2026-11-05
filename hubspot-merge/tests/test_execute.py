from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import pytest

from hsmerge.client import ReadResult, Record
from hsmerge.execute import execute, preview
from hsmerge.types import LogEntry, Pair, Status


@dataclass
class FakeHubSpot:
    """Contacts that are live or absorbed into another, read and merged the way HubSpot does."""

    live: set[str]
    absorbed: dict[str, str] = field(default_factory=dict)
    refuse: set[str] = field(default_factory=set)
    fail_read_after: int | None = None
    reads: int = 0
    merges: list[tuple[str, str]] = field(default_factory=list)

    def batch_read(self, ids: Sequence[str], properties: Sequence[str]) -> ReadResult:
        self.reads += 1
        if self.fail_read_after is not None and self.reads > self.fail_read_after:
            raise TimeoutError("read timed out")
        records = {}
        missing = []
        for contact_id in ids:
            survivor = self.absorbed.get(contact_id, contact_id)
            if survivor not in self.live:
                missing.append(contact_id)
                continue
            merged = tuple(i for i, s in self.absorbed.items() if s == survivor)
            records[survivor] = Record(survivor, merged, {"email": f"{survivor}@example.test"})
        return ReadResult(records, tuple(missing))

    def merge(self, primary_id: str, secondary_id: str) -> Record:
        self.merges.append((primary_id, secondary_id))
        if secondary_id in self.refuse:
            raise TimeoutError("merge timed out")
        self.live.discard(secondary_id)
        self.absorbed[secondary_id] = primary_id
        return Record(primary_id, (secondary_id,), {})


@dataclass
class FakeLog:
    writes: list[list[LogEntry]] = field(default_factory=list)

    def write(self, entries: Sequence[LogEntry]) -> None:
        self.writes.append(list(entries))

    @property
    def rows(self) -> list[LogEntry]:
        return [entry for write in self.writes for entry in write]


def test_a_mixed_chunk_gets_the_right_status_per_pair() -> None:
    hubspot = FakeHubSpot(
        live={"p1", "s1", "p2", "p3", "x3", "p4", "x5", "s5", "s6", "p7", "s7", "p8", "s8"},
        absorbed={"s2": "p2", "s3": "x3", "p5": "x5"},
        refuse={"s7"},
    )
    pairs = [Pair(f"p{n}", f"s{n}") for n in range(1, 9)]
    log = FakeLog()

    counts = execute(pairs, hubspot, log)

    statuses = {entry.pair.secondary_id: entry.outcome.status for entry in log.rows}
    assert statuses == {
        "s1": Status.MERGED,
        "s2": Status.ALREADY_MERGED,
        "s3": Status.MERGED_ELSEWHERE,
        "s4": Status.SECONDARY_MISSING,
        "s5": Status.PRIMARY_MERGED_AWAY,
        "s6": Status.PRIMARY_MISSING,
        "s7": Status.FAILED,
        "s8": Status.MERGED,
    }
    assert hubspot.merges == [("p1", "s1"), ("p7", "s7"), ("p8", "s8")]
    assert counts[Status.MERGED] == 2


def test_a_merge_exception_logs_failed_with_its_error() -> None:
    """A transport error is not a HubSpotError, so nothing upstream catches it."""
    hubspot = FakeHubSpot(live={"p", "s"}, refuse={"s"})
    log = FakeLog()

    execute([Pair("p", "s")], hubspot, log)

    [entry] = log.rows
    assert entry.outcome.status is Status.FAILED
    assert entry.outcome.error == "TimeoutError: merge timed out"


def test_snapshots_are_the_pre_merge_reads() -> None:
    hubspot = FakeHubSpot(live={"p", "s"})
    log = FakeLog()

    execute([Pair("p", "s"), Pair("p", "gone")], hubspot, log)

    merged, missing = log.rows
    assert merged.primary_snapshot == {"email": "p@example.test"}
    assert merged.secondary_snapshot == {"email": "s@example.test"}
    assert missing.secondary_snapshot is None


def test_a_crash_mid_run_leaves_every_finished_chunk_logged() -> None:
    pairs = [Pair(f"p{n}", f"s{n}") for n in range(120)]
    hubspot = FakeHubSpot(live={i for p in pairs for i in (p.primary_id, p.secondary_id)}, fail_read_after=2)
    log = FakeLog()

    with pytest.raises(TimeoutError):
        execute(pairs, hubspot, log)

    assert [len(write) for write in log.writes] == [50, 50]


def test_preview_merges_nothing() -> None:
    hubspot = FakeHubSpot(live={"p1", "s1", "p2"}, absorbed={"s2": "p2"})

    result = list(preview([Pair("p1", "s1"), Pair("p2", "s2")], hubspot))

    assert [outcome.status if outcome else None for _, outcome in result] == [None, Status.ALREADY_MERGED]
    assert hubspot.merges == []
