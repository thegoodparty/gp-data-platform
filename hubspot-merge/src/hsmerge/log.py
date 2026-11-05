"""Appends one row per merge attempt to the job's log table, and the DDL that creates it.

The log is the audit trail of a destructive operation: a merge cannot be undone, so each
attempt is kept, together with the pre-merge read that justified it. Rows are only ever
inserted. This module has no update, delete or merge path at all, and dbt derives each
pair's current state from the rows it holds.

DDL exists only in `create_log_table_sql`/`init_log_table`, and only the init ceremony
(`hsm init-log`) calls it. A run never creates or alters a table, so a lost or renamed log
table fails the run loudly instead of quietly starting a second history beside the first.

The INSERT shape is copied from `reverse-etl/src/retl/sent_log.py` rather than shared:
subprojects do not import each other's code.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol, Self

from .types import Outcome, Pair

# Every INSERT is its own Delta commit (about a second against a live warehouse), so each
# packs as many rows as the warehouse allows: it rejects any statement whose parameters
# total more than this.
PARAM_CHAR_LIMIT = 1_048_576
_CHUNK_CHAR_BUDGET = PARAM_CHAR_LIMIT - 100_000  # headroom for the [] framing

# The log's contract with dbt: column order, name and type as the staging model reads
# them. The DDL and the INSERT are both built from this one list.
_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "string not null"),
    ("object_type", "string not null"),
    ("primary_id", "string not null"),
    ("secondary_id", "string not null"),
    ("status", "string not null"),
    ("survivor_id", "string"),
    ("gp_person_id", "string"),
    ("match_evidence", "string"),
    ("error", "string"),
    ("primary_snapshot", "string"),
    ("secondary_snapshot", "string"),
    ("attempted_at", "timestamp not null"),
)
_LOG_COLUMNS = tuple(name for name, _ in _COLUMNS)

_DDL_COMMENT = "One row per HubSpot merge attempt. Append-only: current state comes from dbt."


class _Connection(Protocol):
    def cursor(self) -> Any: ...


def create_log_table_sql(log_table: str) -> str:
    """The package's only DDL. IF NOT EXISTS only: init must be physically unable to wipe
    an existing table's history, so no statement here can drop, replace or truncate.
    `log_table` is trusted config interpolated directly into the statement -- DB-API
    params cannot bind a table name.
    """
    return (
        f"create table if not exists {log_table} ("
        + ", ".join(f"{name} {type_}" for name, type_ in _COLUMNS)
        + f") using delta comment '{_DDL_COMMENT}'"
    )


def init_log_table(connection: _Connection, log_table: str) -> None:
    """Create `log_table` if it does not exist yet. Safe to rerun: on an existing table the
    statement is a true no-op and every row it holds stays.
    """
    with connection.cursor() as cursor:
        cursor.execute(create_log_table_sql(log_table))


def _insert_rows_sql(log_table: str) -> str:
    """One INSERT that lands every row packed into its `:rows` JSON param.

    The rows travel as ONE JSON string parameter, decoded server-side by from_json. The
    connector's ArrayParameter would be the obvious shape, but a live warehouse binds a
    Python list as an empty array<void>: the insert succeeds and lands zero rows,
    silently. Values are never inlined: quote-escaping SQL by hand is a known trap, and
    json.dumps plus from_json round-trips any string byte-identically.
    """
    schema = "array<struct<" + ",".join(f"{column}:string" for column in _LOG_COLUMNS) + ">>"
    # Rows travel as JSON strings, so the one non-string column needs an explicit cast.
    selected = ", ".join(
        f"cast(r.{column} as timestamp)" if column == "attempted_at" else f"r.{column}"
        for column in _LOG_COLUMNS
    )
    return (
        f"insert into {log_table} ({', '.join(_LOG_COLUMNS)}) "
        f"select {selected} from (select explode(from_json(:rows, '{schema}')) as r)"
    )


def _encode_timestamp(value: datetime) -> str:
    """ISO-8601 in UTC, with the offset spelled out. A bare `yyyy-MM-dd HH:mm:ss` string is
    read as the session's local time, so the same row would land hours off on a warehouse
    configured outside UTC; an explicit offset cannot be misread. Verified against the
    warehouse: from_json keeps the microseconds and the instant.
    """
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _encode_snapshot(snapshot: Mapping[str, Any] | None) -> str | None:
    return None if snapshot is None else json.dumps(snapshot, separators=(",", ":"))


class MergeLogWriter:
    """Buffers attempt rows and appends each full chunk as one INSERT the moment it fills.

    Use as a context manager: exit appends the remainder, including when the caller raises,
    so a run that dies mid-pair keeps every pair it already finished. A hard kill loses at
    most one partial chunk, which the next run re-attempts and logs as `already_merged`.
    """

    def __init__(self, connection: _Connection, log_table: str, *, run_id: str, object_type: str) -> None:
        self._connection = connection
        self._sql = _insert_rows_sql(log_table)
        self._run_id = run_id
        self._object_type = object_type
        self._rows: list[str] = []
        self._size = 0

    def append(
        self,
        pair: Pair,
        outcome: Outcome,
        *,
        primary_snapshot: Mapping[str, Any] | None = None,
        secondary_snapshot: Mapping[str, Any] | None = None,
    ) -> None:
        """Buffer one attempt. The snapshots are the pre-merge reads of each contact, kept so
        a merge that turns out to be wrong can be reconstructed by hand; a contact the read
        never returned has no snapshot.
        """
        row = {
            "run_id": self._run_id,
            "object_type": self._object_type,
            "primary_id": pair.primary_id,
            "secondary_id": pair.secondary_id,
            "status": outcome.status.value,
            "survivor_id": outcome.survivor_id,
            "gp_person_id": pair.gp_person_id,
            "match_evidence": pair.match_evidence,
            "error": outcome.error,
            "primary_snapshot": _encode_snapshot(primary_snapshot),
            "secondary_snapshot": _encode_snapshot(secondary_snapshot),
            "attempted_at": _encode_timestamp(datetime.now(UTC)),
        }
        encoded = json.dumps(row, separators=(",", ":"))
        # Buffer the new row before the flush, so a failed INSERT cannot drop the row for the
        # merge that just happened: `__exit__` retries whatever the buffer holds, and the batch
        # the warehouse refused goes back in alongside it.
        if self._rows and self._size + len(encoded) + 1 > _CHUNK_CHAR_BUDGET:
            batch = self._rows
            self._rows = [encoded]
            try:
                self._insert(batch)
            except Exception:
                self._rows = [*batch, encoded]
                self._size = sum(len(r) + 1 for r in self._rows)
                raise
            self._size = len(encoded) + 1
        else:
            self._rows.append(encoded)
            self._size += len(encoded) + 1

    def _insert(self, rows: list[str]) -> None:
        if not rows:
            return
        with self._connection.cursor() as cursor:
            cursor.execute(self._sql, {"rows": "[" + ",".join(rows) + "]"})

    def _flush(self) -> None:
        self._insert(self._rows)
        self._rows, self._size = [], 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        if exc is None:
            self._flush()
            return
        try:
            self._flush()
        except Exception as flush_exc:
            # Keep the caller's error as the one that surfaces: it is the real cause, and
            # the unlogged pairs are simply re-attempted next run.
            exc.add_note(f"flush on exit also failed: {flush_exc}")
