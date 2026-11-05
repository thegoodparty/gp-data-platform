"""Reads the merge pairs from the configured relation.

The relation carries every eligibility rule: the queue view keeps only the rows that may
merge now, so this module applies no filter of its own. Its only choices are how many rows
to read and in what order.

Oldest elections first, because those contacts matter least and a wrong merge there costs
the least to review. A person with no election date goes last, since an unknown date is not
evidence of an old election.
"""

from __future__ import annotations

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
        # `relation` is trusted configuration, not user input, and DB-API parameters cannot
        # bind a relation name. The cap is a parameter, and the ORDER BY applies before it,
        # so the cap keeps the lowest-risk rows rather than an arbitrary slice.
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
