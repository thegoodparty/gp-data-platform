"""What one pair's batch read and merge answer mean, as a status.

The statuses that look like races are settled before the merge, by the read: a merged-away
id reads back as its survivor, and the merge endpoint refuses such a pair with a 400. Both
functions are pure, and both take what the client parsed.
"""

from __future__ import annotations

from .client import ReadResult, Record
from .types import Outcome, Pair, Status


def classify_read(pair: Pair, read: ReadResult) -> Outcome | None:
    """The status the read settles, or None when the pair is two live contacts to merge."""
    resolved = read.resolved
    primary = resolved.get(pair.primary_id)
    if primary is None:
        return Outcome(Status.PRIMARY_MISSING)
    if primary != pair.primary_id:
        # The primary was absorbed elsewhere, so no merge of this pair can land. Left for
        # the next read rather than failed: the mart re-ranks once it syncs.
        return Outcome(Status.PRIMARY_MERGED_AWAY)
    secondary = resolved.get(pair.secondary_id)
    if secondary is None:
        return Outcome(Status.SECONDARY_MISSING)
    if secondary == pair.primary_id:
        return Outcome(Status.ALREADY_MERGED, survivor_id=pair.primary_id)
    if secondary != pair.secondary_id:
        return Outcome(Status.MERGED_ELSEWHERE, survivor_id=secondary)
    return None


def classify_merge(pair: Pair, survivor: Record) -> Outcome:
    """The status of a merge the read allowed through.

    `survivor` is the record the merge endpoint returned. The client raises `HubSpotError`
    when HubSpot refuses the pair, so the executor logs that as `failed` and never reaches
    here.
    """
    if survivor.id == pair.primary_id:
        return Outcome(Status.MERGED, survivor_id=survivor.id)
    # Only the ID-preservation beta makes the returned id the primary's; without it the
    # survivor is a new record and every downstream model keys on the old id.
    return Outcome(Status.ID_CHANGED, survivor_id=survivor.id)
