from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from hsmerge.log import (
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
    assert connection.tables[LOG_TABLE].columns == (
        "run_id",
        "object_type",
        "primary_id",
        "secondary_id",
        "status",
        "survivor_id",
        "gp_person_id",
        "match_evidence",
        "error",
        "primary_snapshot",
        "secondary_snapshot",
        "attempted_at",
    )


def test_create_log_table_sql_can_only_create() -> None:
    """Catches: any DDL that could replace an existing table and lose its history."""
    sql = create_log_table_sql(LOG_TABLE).lower()
    assert sql.startswith(f"create table if not exists {LOG_TABLE.lower()} (")
    assert sql.count("table") == 1


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


def test_writer_keeps_the_current_pair_when_a_mid_run_flush_fails() -> None:
    """Catches: flushing the full batch before buffering the new row, so a failed INSERT drops
    the row for the merge that just happened. A merge cannot be undone, so that row is the one
    most worth keeping, and the exit flush retries only what the buffer still holds."""
    connection = FakeConnection(insert_failures=1)
    init_log_table(connection, LOG_TABLE)
    snapshot = {"id": "p", "properties": {"note": "x" * 2_000}}
    merged = None

    with (
        pytest.raises(RuntimeError, match="insert failed"),
        _writer(connection) as writer,
    ):
        for n in range(1_000):
            merged = _pair(n)
            writer.append(merged, Outcome(status=Status.MERGED, survivor_id="p"), primary_snapshot=snapshot)

    assert merged is not None
    assert merged.secondary_id in {row["secondary_id"] for row in connection.tables[LOG_TABLE].rows}


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
