from __future__ import annotations

from typing import Any

import pytest

from hsmerge.pairs import PairConflictError, check_pairs, read_pairs
from hsmerge.types import Pair

RELATION = "goodparty_data_catalog.dbt_staging.hubspot_contact_merge_queue"


class _FakeCursor:
    def __init__(self, connection: _FakeConnection):
        self._connection = connection

    def execute(self, sql: str, parameters: dict[str, Any] | None = None) -> None:
        self._connection.executed_sql = sql
        self._connection.executed_parameters = parameters or {}

    def fetchall(self) -> list[dict[str, Any]]:
        return self._connection.rows

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _FakeConnection:
    """The rows a read returns, and the statement it was asked for."""

    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.rows = rows or []
        self.executed_sql = ""
        self.executed_parameters: dict[str, Any] = {}

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def test_sql_selects_only_the_columns_the_job_uses() -> None:
    """Selecting everything invites a later reader to filter on the relation, which is
    the queue's job and would hide an eligibility rule the job then fails to apply."""
    assert _normalized(_executed_sql()).startswith(
        "select primary_hs_contact_id, hs_contact_id, gp_person_id, match_evidence from"
    )


def test_sql_applies_no_filter_of_its_own() -> None:
    """The queue view applies every eligibility rule; a WHERE clause here would narrow
    the job's input to something dbt never approved."""
    assert " where " not in _normalized(_executed_sql()).lower()


def test_sql_orders_by_election_date_oldest_first_with_nulls_last() -> None:
    """The order is the job's whole risk policy, and it has to apply before the cap: a
    statement that sorted after capping, or sorted nulls first, would merge the newest
    elections or the dateless contacts ahead of the ones that matter least."""
    assert "order by latest_election_date nulls last, hs_contact_id limit :limit" in _normalized(
        _executed_sql()
    )


def test_sql_reads_the_configured_relation() -> None:
    """A relation from config being dropped, so a narrowed or sandbox run silently read
    the production queue instead."""
    assert f"from {RELATION} " in _executed_sql()


def test_read_pairs_binds_the_limit_rather_than_inlining_it() -> None:
    """The cap reaching the statement as text, where it is one edit away from a read of
    the whole queue."""
    connection = _FakeConnection()
    read_pairs(connection, RELATION, 50)
    assert connection.executed_parameters == {"limit": 50}


def test_read_pairs_maps_rows_to_pairs() -> None:
    """A column read into the wrong field, which would swap the primary and the secondary
    and merge the survivor into the contact it should absorb."""
    connection = _FakeConnection(
        [
            {
                "primary_hs_contact_id": "1001",
                "hs_contact_id": "1002",
                "gp_person_id": "0b1f-person",
                "match_evidence": "same_phone",
            }
        ]
    )
    assert read_pairs(connection, RELATION, 50) == [
        Pair(
            primary_id="1001",
            secondary_id="1002",
            gp_person_id="0b1f-person",
            match_evidence="same_phone",
        )
    ]


def test_read_pairs_allows_a_pair_without_audit_fields() -> None:
    """A hand-built pair table for a narrowed run leaves the audit columns null, and a
    reader that insisted on them would refuse a legitimate input relation."""
    connection = _FakeConnection(
        [
            {
                "primary_hs_contact_id": "1001",
                "hs_contact_id": "1002",
                "gp_person_id": None,
                "match_evidence": None,
            }
        ]
    )
    assert read_pairs(connection, RELATION, 50) == [Pair(primary_id="1001", secondary_id="1002")]


def test_read_pairs_returns_nothing_for_an_empty_relation() -> None:
    """An exhausted queue is the normal end state; it must read as no pairs rather than as
    an error the scheduled run reports."""
    assert read_pairs(_FakeConnection(), RELATION, 50) == []


def _executed_sql() -> str:
    connection = _FakeConnection()
    read_pairs(connection, RELATION, 50)
    return connection.executed_sql


@pytest.mark.parametrize(
    "pairs",
    [
        [Pair("p1", "s1"), Pair("p2", "s1")],
        [Pair("p1", "s1"), Pair("t1", "p1")],
    ],
    ids=["repeated secondary", "chain"],
)
def test_pairs_that_depend_on_each_other_are_refused(pairs: list[Pair]) -> None:
    with pytest.raises(PairConflictError):
        check_pairs(pairs)


def test_a_primary_with_several_secondaries_is_allowed() -> None:
    check_pairs([Pair("p1", "s1"), Pair("p1", "s2")])
