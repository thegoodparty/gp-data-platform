"""The values the job passes between its parts: one pair to merge, the status vocabulary
its log records, and what happened to one pair.

The status strings are the log's contract. dbt reads them back (`hubspot_contact_merge_queue`
excludes pairs by status), so a value here is a value in the warehouse, not an internal
label.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Status(StrEnum):
    """The outcome of one attempted pair. Every pair attempted gets exactly one.

    Which of these the queue excludes is dbt's rule, not this package's; the table in
    the design doc is the reference for both sides.
    """

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
    """One merge operation: the contact to absorb and the contact that must survive.

    `gp_person_id` and `match_evidence` are audit fields carried from the queue into the
    log; the merge itself does not use them, so a pair built by hand may omit them.
    """

    primary_id: str
    secondary_id: str
    gp_person_id: str | None = None
    match_evidence: str | None = None


@dataclass(frozen=True)
class Outcome:
    """What became of one pair, ready to log.

    `survivor_id` is the contact the secondary resolved to or the id the merge returned;
    `error` carries HubSpot's own message on `failed`, and stays None otherwise.
    """

    status: Status
    survivor_id: str | None = None
    error: str | None = None
