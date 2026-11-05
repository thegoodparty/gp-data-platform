from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

from hsmerge.log import MergeLog, create_log_table_sql, init_log_table
from hsmerge.types import LogEntry, Outcome, Pair, Status
from tests._warehouse import FakeWarehouse

TABLE = "goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log"


def _log(warehouse: FakeWarehouse, now: datetime | None = None) -> MergeLog:
    init_log_table(warehouse, TABLE)
    clock = (lambda: now) if now else (lambda: datetime.now(UTC))
    return MergeLog(warehouse, TABLE, run_id="run-1", object_type="contacts", now=clock)


def _entry(n: int, **snapshot: str) -> LogEntry:
    return LogEntry(
        Pair(f"p{n}", f"s{n}", gp_person_id=f"person-{n}", match_evidence="same_phone"),
        Outcome(Status.MERGED, survivor_id=f"p{n}"),
        primary_snapshot=snapshot or None,
    )


def test_the_ddl_only_creates() -> None:
    assert create_log_table_sql(TABLE).startswith(f"create table if not exists {TABLE} (")


def test_a_row_carries_the_pair_the_outcome_and_the_snapshots() -> None:
    warehouse = FakeWarehouse()
    entry = LogEntry(
        Pair("p1", "s1", gp_person_id="person-1", match_evidence="same_phone"),
        Outcome(Status.FAILED, error="HubSpot answered 400"),
        primary_snapshot={"email": "a@example.test"},
    )

    _log(warehouse).write([entry])

    [row] = warehouse.rows[TABLE]
    assert {key: value for key, value in row.items() if key != "attempted_at"} == {
        "run_id": "run-1",
        "object_type": "contacts",
        "primary_id": "p1",
        "secondary_id": "s1",
        "status": "failed",
        "survivor_id": None,
        "gp_person_id": "person-1",
        "match_evidence": "same_phone",
        "error": "HubSpot answered 400",
        "primary_snapshot": json.dumps({"email": "a@example.test"}, separators=(",", ":")),
        "secondary_snapshot": None,
    }


def test_attempted_at_is_utc_with_an_explicit_offset() -> None:
    """Spark reads a timestamp string without an offset in the session's time zone."""
    warehouse = FakeWarehouse()
    eastern = datetime(2026, 11, 5, 9, 0, tzinfo=timezone(timedelta(hours=-5)))

    _log(warehouse, now=eastern).write([_entry(1)])

    assert warehouse.rows[TABLE][0]["attempted_at"] == "2026-11-05T14:00:00.000000+00:00"


def test_a_large_write_splits_under_the_parameter_limit() -> None:
    warehouse = FakeWarehouse()

    _log(warehouse).write([_entry(n, note="x" * 2_000) for n in range(1_000)])

    assert warehouse.inserts > 1
    assert len(warehouse.rows[TABLE]) == 1_000
