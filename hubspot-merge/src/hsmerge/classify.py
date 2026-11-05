"""What one pair's batch read and merge answer mean, as a status.

The statuses that look like races are settled before the merge, by the read: a merged-away
id reads back as its survivor, and the merge endpoint refuses such a pair with a 400. Both
functions are pure, over the response shapes recorded in `reverse-etl/probes/README.md`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from .types import Outcome, Pair, Status

# The properties a survivor names its absorbed contact ids in. The read's property list
# belongs to the caller, so any one of these may be the only one present.
MERGE_HISTORY_PROPERTIES = ("hs_merged_object_ids", "hs_all_contact_vids", "hs_calculated_merged_vids")


@dataclass(frozen=True)
class Read:
    """One batch read, resolved against every id it can speak for.

    A read answers per resolved object: one record per distinct object, no echo of the ids
    asked for, and not in the order asked. Attribution therefore goes through the records
    themselves. `resolved` maps a contact id to the record it came back as, and an id absent
    from it did not resolve at all.

    A read has to ask for at least one of `MERGE_HISTORY_PROPERTIES`. Without one, an
    absorbed id appears nowhere in the response and reads as missing instead of as merged.
    """

    resolved: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_body(cls, body: Mapping[str, Any]) -> Read:
        resolved: dict[str, str] = {}
        for record in body.get("results") or []:
            record_id = str(record["id"])
            resolved[record_id] = record_id
            properties = record.get("properties") or {}
            for prop in MERGE_HISTORY_PROPERTIES:
                for merged_id in _split_ids(properties.get(prop)):
                    resolved[merged_id] = record_id
        return cls(resolved)


class MergeResponse(Protocol):
    """A merge call's answer, as much of it as classification reads."""

    @property
    def status_code(self) -> int: ...

    @property
    def body(self) -> Mapping[str, Any]: ...


def classify_read(pair: Pair, read: Read) -> Outcome | None:
    """The status the read settles, or None when the pair is two live contacts to merge."""
    primary = read.resolved.get(pair.primary_id)
    if primary is None:
        return Outcome(Status.PRIMARY_MISSING)
    if primary != pair.primary_id:
        # The primary was absorbed elsewhere, so no merge of this pair can land. Left for
        # the next read rather than failed: the mart re-ranks once it syncs.
        return Outcome(Status.PRIMARY_MERGED_AWAY)
    secondary = read.resolved.get(pair.secondary_id)
    if secondary is None:
        return Outcome(Status.SECONDARY_MISSING)
    if secondary == pair.primary_id:
        return Outcome(Status.ALREADY_MERGED, survivor_id=pair.primary_id)
    if secondary != pair.secondary_id:
        return Outcome(Status.MERGED_ELSEWHERE, survivor_id=secondary)
    return None


def classify_merge(pair: Pair, response: MergeResponse) -> Outcome:
    """The status of a merge the read allowed through.

    Every refusal lands on `failed`: an id that does not exist answers 404, an
    already-resolved pair answers 400, and the read is what names those two. Reaching either
    here means the read was wrong about the pair, which a retry can still settle.
    """
    if response.status_code >= 300:
        return Outcome(Status.FAILED, error=_refusal(response))

    returned = str(response.body.get("id") or "")
    if returned == pair.primary_id:
        return Outcome(Status.MERGED, survivor_id=returned)
    if returned:
        # Only the ID-preservation beta makes the returned id the primary's; without it the
        # survivor is a new record and every downstream model keys on the old id.
        return Outcome(Status.ID_CHANGED, survivor_id=returned)
    return Outcome(Status.FAILED, error="merge answered without an id")


def _refusal(response: MergeResponse) -> str:
    code = f"{response.status_code} {response.body.get('category') or ''}".strip()
    message = response.body.get("message")
    return f"{code}: {message}" if message else code


def _split_ids(value: Any) -> list[str]:
    """One history property, split into contact ids.

    Semicolon-separated, and `hs_calculated_merged_vids` suffixes each id with its merge
    timestamp, so the suffix is dropped.
    """
    if not value:
        return []
    return [part.split(":", 1)[0] for part in str(value).split(";") if part]
