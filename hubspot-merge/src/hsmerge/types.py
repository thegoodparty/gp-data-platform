"""The values the job passes between its parts: one pair, its status, and what happened.

The status strings are the log's contract: dbt filters the queue on them, so changing one
changes a warehouse value.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Status(StrEnum):
    MERGED = "merged"
    ALREADY_MERGED = "already_merged"
    MERGED_ELSEWHERE = "merged_elsewhere"
    SECONDARY_MISSING = "secondary_missing"
    PRIMARY_MERGED_AWAY = "primary_merged_away"
    PRIMARY_MISSING = "primary_missing"
    ID_CHANGED = "id_changed"
    FAILED = "failed"


@dataclass(frozen=True)
class Pair:
    """One merge: `secondary_id` is absorbed into `primary_id`.

    The audit fields are carried from the queue into the log; a pair built by hand may
    omit them.
    """

    primary_id: str
    secondary_id: str
    gp_person_id: str | None = None
    match_evidence: str | None = None


@dataclass(frozen=True)
class Outcome:
    """What became of one pair, ready to log. `error` is set only on `failed`."""

    status: Status
    survivor_id: str | None = None
    error: str | None = None
