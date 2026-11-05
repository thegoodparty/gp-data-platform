"""Reads the merge pairs from the configured relation.

The relation carries every eligibility rule: the queue view keeps only the rows that may
merge now, so this module applies no filter of its own. Its only choices are how many rows
to read and in what order.

Oldest elections first, because those contacts matter least and a wrong merge there costs
the least to review. A person with no election date goes last, since an unknown date is not
evidence of an old election.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Protocol

from .types import Pair


class _Cursor(Protocol):
    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> None: ...

    def fetchall(self) -> Any: ...

    def __enter__(self) -> _Cursor: ...

    def __exit__(self, *exc: object) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...


def read_pairs(connection: _Connection, relation: str, limit: int) -> list[Pair]:
    """Up to `limit` pairs from `relation`, oldest elections first."""
    with connection.cursor() as cursor:
        # DB-API parameters cannot bind a relation name; `Config` checks its shape. The
        # ORDER BY applies before the cap, so the cap keeps the lowest-risk rows.
        cursor.execute(
            "select primary_hs_contact_id, hs_contact_id, gp_person_id, match_evidence "
            f"from {relation} "
            "order by latest_election_date nulls last, hs_contact_id "
            "limit :limit",
            {"limit": limit},
        )
        rows = cursor.fetchall()

    return [
        Pair(
            primary_id=row["primary_hs_contact_id"],
            secondary_id=row["hs_contact_id"],
            gp_person_id=row["gp_person_id"],
            match_evidence=row["match_evidence"],
        )
        for row in rows
    ]


class PairConflictError(ValueError):
    pass


def check_pairs(pairs: list[Pair]) -> None:
    """Refuse pairs whose merges would depend on each other.

    The executor classifies a chunk from one read taken before any of its merges, so a
    secondary listed twice, or an id that is both a primary and a secondary, would merge
    on a stale read.
    """
    secondaries = Counter(pair.secondary_id for pair in pairs)
    repeated = sorted(id_ for id_, count in secondaries.items() if count > 1)
    both = sorted(secondaries.keys() & {pair.primary_id for pair in pairs})
    if repeated or both:
        raise PairConflictError(
            f"secondary listed more than once: {repeated}; id is both primary and secondary: {both}"
        )


def read_merged_pairs(connection: _Connection, log_table: str, run_id: str) -> list[Pair]:
    """The pairs one run logged as `merged`, for `hsm verify`."""
    with connection.cursor() as cursor:
        cursor.execute(
            f"select primary_id, secondary_id from {log_table} "
            "where run_id = :run_id and status = 'merged' order by secondary_id",
            {"run_id": run_id},
        )
        rows = cursor.fetchall()
    return [Pair(primary_id=row["primary_id"], secondary_id=row["secondary_id"]) for row in rows]
