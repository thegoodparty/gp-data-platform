from __future__ import annotations

import pytest

from hsmerge.execute import execute, preview, verify
from hsmerge.types import Pair, Status
from tests._fakes import FakeHubSpot, FakeLog


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


def test_an_id_changed_stops_the_run_after_its_chunk() -> None:
    """Without ID preservation, every later merge would change its id too."""
    pairs = [Pair(f"p{n}", f"s{n}") for n in range(120)]
    hubspot = FakeHubSpot(live={i for p in pairs for i in (p.primary_id, p.secondary_id)}, new_ids=True)
    log = FakeLog()

    counts = execute(pairs, hubspot, log)

    assert [len(write) for write in log.writes] == [50]
    assert counts[Status.ID_CHANGED] == 50


def test_preview_merges_nothing() -> None:
    hubspot = FakeHubSpot(live={"p1", "s1", "p2"}, absorbed={"s2": "p2"})

    result = list(preview([Pair("p1", "s1"), Pair("p2", "s2")], hubspot))

    assert [outcome.status if outcome else None for _, outcome in result] == [None, Status.ALREADY_MERGED]
    assert hubspot.merges == []


def test_verify_names_a_pair_hubspot_does_not_show_as_merged() -> None:
    hubspot = FakeHubSpot(live={"p1", "p2", "s2"}, absorbed={"s1": "p1"})

    problems = list(verify([Pair("p1", "s1"), Pair("p2", "s2")], hubspot))

    assert problems == [(Pair("p2", "s2"), "the secondary does not resolve to the primary")]
