"""The merge log: an append-only Delta table with one row per attempted pair.

This module only creates the table and inserts rows. dbt derives each pair's current
state from the rows.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from .types import LogEntry

# The warehouse rejects a statement whose parameters total more than this.
PARAM_CHAR_LIMIT = 1_048_576
_CHUNK_CHAR_BUDGET = PARAM_CHAR_LIMIT - 100_000

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
_NAMES = tuple(name for name, _ in _COLUMNS)


class _Connection(Protocol):
    def cursor(self) -> Any: ...


def create_log_table_sql(log_table: str) -> str:
    """`create table if not exists`, so a rerun cannot replace a table and lose its rows."""
    columns = ", ".join(f"{name} {type_}" for name, type_ in _COLUMNS)
    return (
        f"create table if not exists {log_table} ({columns}) using delta "
        "comment 'One row per HubSpot merge attempt. Append-only.'"
    )


def init_log_table(connection: _Connection, log_table: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(create_log_table_sql(log_table))


def _insert_sql(log_table: str) -> str:
    # Rows travel as one JSON string parameter. The connector binds a Python list as an
    # empty array<void>, which inserts zero rows without an error.
    schema = "array<struct<" + ",".join(f"{name}:string" for name in _NAMES) + ">>"
    selected = ", ".join(
        f"cast(r.{name} as timestamp)" if name == "attempted_at" else f"r.{name}" for name in _NAMES
    )
    return (
        f"insert into {log_table} ({', '.join(_NAMES)}) "
        f"select {selected} from (select explode(from_json(:rows, '{schema}')) as r)"
    )


def _snapshot(properties: Mapping[str, Any] | None) -> str | None:
    return None if properties is None else json.dumps(properties, separators=(",", ":"))


class MergeLog:
    """Writes log rows for one run. Each `write` is one INSERT, or more if the rows exceed
    the parameter limit."""

    def __init__(
        self,
        connection: _Connection,
        log_table: str,
        *,
        run_id: str,
        object_type: str,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._connection = connection
        self._sql = _insert_sql(log_table)
        self._run_id = run_id
        self._object_type = object_type
        self._now = now

    def check(self) -> None:
        """Insert zero rows, so a missing table or grant fails before the first merge."""
        self._insert([])

    def write(self, entries: Sequence[LogEntry]) -> None:
        # An explicit UTC offset, since a bare timestamp string is read in the session's
        # time zone.
        attempted_at = self._now().astimezone(UTC).isoformat(timespec="microseconds")
        chunk: list[str] = []
        size = 0
        for entry in entries:
            row = json.dumps(self._row(entry, attempted_at), separators=(",", ":"))
            if chunk and size + len(row) + 1 > _CHUNK_CHAR_BUDGET:
                self._insert(chunk)
                chunk, size = [], 0
            chunk.append(row)
            size += len(row) + 1
        if chunk:
            self._insert(chunk)

    def _row(self, entry: LogEntry, attempted_at: str) -> dict[str, str | None]:
        return {
            "run_id": self._run_id,
            "object_type": self._object_type,
            "primary_id": entry.pair.primary_id,
            "secondary_id": entry.pair.secondary_id,
            "status": entry.outcome.status.value,
            "survivor_id": entry.outcome.survivor_id,
            "gp_person_id": entry.pair.gp_person_id,
            "match_evidence": entry.pair.match_evidence,
            "error": entry.outcome.error,
            "primary_snapshot": _snapshot(entry.primary_snapshot),
            "secondary_snapshot": _snapshot(entry.secondary_snapshot),
            "attempted_at": attempted_at,
        }

    def _insert(self, rows: list[str]) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(self._sql, {"rows": "[" + ",".join(rows) + "]"})
