from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from hsmerge.log import (
    LOG_COLUMNS,
    PARAM_CHAR_LIMIT,
    MergeLogWriter,
    create_log_table_sql,
    init_log_table,
)
from hsmerge.types import Outcome, Pair, Status
from tests._fakes import FakeConnection

LOG_TABLE = "goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log"


def _writer(connection: FakeConnection) -> MergeLogWriter:
    return MergeLogWriter(connection, LOG_TABLE, run_id="run-1", object_type="contacts")


def _pair(n: int) -> Pair:
    return Pair(
        primary_id=f"p{n}",
        secondary_id=f"s{n}",
        gp_person_id=f"person-{n}",
        match_evidence="same_phone",
    )


def test_init_log_table_declares_the_columns_the_writer_inserts() -> None:
    """Catches: one side of the DDL/INSERT pair drifting, which a live warehouse refuses at
    the first flush instead of at init."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)
    assert connection.tables[LOG_TABLE].columns == LOG_COLUMNS


def test_init_log_table_is_a_no_op_against_an_existing_table() -> None:
    """Catches: a second init against a table that already holds rows replacing it."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)
    with _writer(connection) as writer:
        writer.append(_pair(1), Outcome(status=Status.MERGED, survivor_id="p1"))

    init_log_table(connection, LOG_TABLE)

    assert len(connection.tables[LOG_TABLE].rows) == 1


def test_create_log_table_sql_can_only_create() -> None:
    """Catches: any DDL that could replace an existing table and lose its history."""
    sql = create_log_table_sql(LOG_TABLE).lower()
    assert sql.startswith(f"create table if not exists {LOG_TABLE.lower()} (")
    assert sql.count("table") == 1
    for destructive in ("drop", "replace", "truncate", "delete", "alter", "insert"):
        assert destructive not in sql


def test_writer_appends_one_row_per_pair_with_its_outcome() -> None:
    """Catches: a column being mapped to the wrong value, so the log misreports a merge."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)
    pair = Pair(
        primary_id="p1",
        secondary_id="s1",
        gp_person_id="person-1",
        match_evidence="same_phone",
    )

    with _writer(connection) as writer:
        writer.append(
            pair,
            Outcome(status=Status.MERGED, survivor_id="p1"),
            primary_snapshot={"id": "p1", "email": "a@example.test"},
            secondary_snapshot={"id": "s1"},
        )

    [row] = connection.tables[LOG_TABLE].rows
    assert row["run_id"] == "run-1"
    assert row["object_type"] == "contacts"
    assert row["primary_id"] == "p1"
    assert row["secondary_id"] == "s1"
    assert row["status"] == "merged"
    assert row["survivor_id"] == "p1"
    assert row["gp_person_id"] == "person-1"
    assert row["match_evidence"] == "same_phone"
    assert row["error"] is None
    assert json.loads(row["primary_snapshot"]) == {"id": "p1", "email": "a@example.test"}
    assert json.loads(row["secondary_snapshot"]) == {"id": "s1"}


def test_writer_records_a_failure_with_its_error_and_no_survivor() -> None:
    """Catches: a failed attempt logging as if it had a survivor, or dropping the reason."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)

    with _writer(connection) as writer:
        writer.append(_pair(1), Outcome(status=Status.FAILED, error="hubspot 500"))

    [row] = connection.tables[LOG_TABLE].rows
    assert row["status"] == "failed"
    assert row["error"] == "hubspot 500"
    assert row["survivor_id"] is None
    assert row["primary_snapshot"] is None
    assert row["secondary_snapshot"] is None


def test_attempted_at_carries_an_explicit_utc_offset() -> None:
    """Catches: a bare `yyyy-MM-dd HH:mm:ss` string, which Spark reads as the session's local
    time, so the same row lands hours off when the warehouse runs outside UTC."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)

    with _writer(connection) as writer:
        writer.append(_pair(1), Outcome(status=Status.MERGED, survivor_id="p1"))

    attempted_at = connection.tables[LOG_TABLE].rows[0]["attempted_at"]
    assert datetime.fromisoformat(attempted_at).utcoffset() == timedelta(0)


def test_writer_packs_many_rows_into_one_insert() -> None:
    """Catches: one INSERT per pair. Every INSERT is its own Delta commit, so logging a
    chunk pair by pair would spend longer committing than the merges took."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)

    with _writer(connection) as writer:
        for n in range(2_000):
            writer.append(_pair(n), Outcome(status=Status.ALREADY_MERGED, survivor_id="p0"))

    assert connection.insert_call_count == 1
    assert len(connection.tables[LOG_TABLE].rows) == 2_000


def test_writer_flushes_a_full_chunk_before_the_context_exits() -> None:
    """Catches: buffering the whole run, so a crash mid-run loses every finished pair.
    A production snapshot is far larger than a bare row, so a few hundred fill a chunk."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)
    snapshot = {"id": "p", "properties": {"note": "x" * 2_000}}

    with _writer(connection) as writer:
        for n in range(2_000):
            writer.append(_pair(n), Outcome(status=Status.MERGED, survivor_id="p"), primary_snapshot=snapshot)
        flushed_before_exit = len(connection.tables[LOG_TABLE].rows)

    assert 0 < flushed_before_exit < 2_000
    assert len(connection.tables[LOG_TABLE].rows) == 2_000


def test_writer_splits_chunks_under_the_parameter_size_limit() -> None:
    """Catches: one statement carrying more than the warehouse's per-query parameter limit
    (1,048,576 characters in total), which it rejects outright, so a chunk of finished
    merges would go unlogged."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)
    snapshot = {"id": "p", "properties": {"note": "x" * 900}}

    with _writer(connection) as writer:
        for n in range(5_000):  # about 4.7M characters of snapshots in all
            writer.append(_pair(n), Outcome(status=Status.MERGED, survivor_id="p"), primary_snapshot=snapshot)

    assert connection.insert_call_count > 1
    assert all(size <= PARAM_CHAR_LIMIT for size in connection.param_sizes)
    assert len(connection.tables[LOG_TABLE].rows) == 5_000


def test_writer_flushes_the_remainder_when_the_caller_raises() -> None:
    """Catches: a run that dies on its second pair losing the first, which the log is there
    to record before anything else can go wrong."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)

    with pytest.raises(RuntimeError, match="hubspot 401"), _writer(connection) as writer:
        writer.append(_pair(1), Outcome(status=Status.MERGED, survivor_id="p1"))
        writer.append(_pair(2), Outcome(status=Status.MERGED, survivor_id="p2"))
        raise RuntimeError("hubspot 401")

    assert [row["secondary_id"] for row in connection.tables[LOG_TABLE].rows] == ["s1", "s2"]


def test_writer_keeps_the_original_error_when_the_exit_flush_also_fails() -> None:
    """Catches: a failed flush on exit replacing the run's own error, so the alert names a
    warehouse INSERT failure instead of the real cause."""
    connection = FakeConnection()  # no table: the flush raises

    with (
        pytest.raises(RuntimeError, match="hubspot 401") as caught,
        _writer(connection) as writer,
    ):
        writer.append(_pair(1), Outcome(status=Status.MERGED, survivor_id="p1"))
        raise RuntimeError("hubspot 401")

    assert any("flush on exit also failed" in note for note in caught.value.__notes__)


def test_writer_issues_no_statement_other_than_insert() -> None:
    """Catches: a correction or cleanup statement creeping into the writer, which would break
    the append-only rule every downstream model derives current state from."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)

    with _writer(connection) as writer:
        writer.append(_pair(1), Outcome(status=Status.MERGED, survivor_id="p1"))

    writer_statements = connection.executed_sql[1:]  # the first is init's DDL
    assert all(sql.lower().startswith("insert into ") for sql in writer_statements)


def test_writer_round_trips_values_that_need_escaping() -> None:
    """Catches: a string being altered on the way in (quotes, backslashes, non-ASCII, a
    newline), which would make the audit row disagree with what HubSpot was sent."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)
    pair = Pair(
        primary_id="p'1",
        secondary_id="s\\1",
        gp_person_id="person-1",
        match_evidence='name == "Zoë 🙂"',
    )

    with _writer(connection) as writer:
        writer.append(
            pair,
            Outcome(status=Status.FAILED, error="bad 'thing' \\ 🙂\nsecond line"),
            primary_snapshot={"properties": {"lastname": "Zoë O'Brien \\ 🙂", "note": "a\nb"}},
        )

    [row] = connection.tables[LOG_TABLE].rows
    assert row["primary_id"] == "p'1"
    assert row["secondary_id"] == "s\\1"
    assert row["match_evidence"] == 'name == "Zoë 🙂"'
    assert row["error"] == "bad 'thing' \\ 🙂\nsecond line"
    assert json.loads(row["primary_snapshot"]) == {
        "properties": {"lastname": "Zoë O'Brien \\ 🙂", "note": "a\nb"}
    }


def test_two_runs_append_to_one_history() -> None:
    """Catches: a second run's rows replacing the first's, which would destroy the audit trail."""
    connection = FakeConnection()
    init_log_table(connection, LOG_TABLE)

    for run_id in ("run-1", "run-2"):
        with MergeLogWriter(connection, LOG_TABLE, run_id=run_id, object_type="contacts") as writer:
            writer.append(_pair(1), Outcome(status=Status.MERGED, survivor_id="p1"))

    assert [row["run_id"] for row in connection.tables[LOG_TABLE].rows] == ["run-1", "run-2"]
