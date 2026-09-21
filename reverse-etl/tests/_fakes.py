"""Shared fakes: in-memory stand-ins for a Databricks connection and an HTTP transport.

Fakes over mocks (ai-rules/test-engineer.md #6): these behave like the real thing at
the boundary retl talks to, so a test failure means retl's own logic is wrong, not
that a mock's script drifted from what the real dependency does.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from retl.sent_log import FLOW_ID_PROPERTY

_CREATE_TABLE_RE = re.compile(
    r"create table if not exists (?P<table>\S+) .*tblproperties\s*\('retl\.flow_id'\s*=\s*'(?P<flow_id>[^']*)'\)",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class FakeTable:
    """One in-memory table: its rows and its TBLPROPERTIES."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)


def stamped_table(flow_id: str, rows: list[dict[str, Any]] | None = None) -> FakeTable:
    """A FakeTable already stamped for `flow_id`, as `init_log_table` would leave it."""
    return FakeTable(rows=list(rows or []), properties={FLOW_ID_PROPERTY: flow_id})


class FakeCursor:
    """Serves every statement shape retl issues: a flat source `select *`; sent_log's
    create-if-not-exists, tblproperties read, latest-per-key read, and chunked
    multi-row insert; and the existence probe `init_log_table` uses for its report.

    Every statement is looked up against `connection.tables` by name: a name absent
    from that dict is exactly a missing table, and any non-create statement against
    one raises -- there is no separate "missing table" toggle because an absent key
    already means that on its own.
    """

    def __init__(self, *, connection: FakeConnection):
        self._connection = connection
        self._result_columns: list[str] = []
        self._result_rows: list[dict[str, Any]] = []
        self._position = 0

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        params = params or {}
        self._connection.execute_call_count += 1
        self._connection.executed_sql.append(sql)
        lowered = sql.lower()
        if lowered.startswith("create table if not exists"):
            self._execute_create_table(sql)
        elif lowered.startswith("show tblproperties"):
            self._execute_show_tblproperties(sql)
        elif lowered.startswith("select 1 from"):
            self._execute_existence_probe(sql)
        elif lowered.startswith("select * from"):
            self._execute_select_source()
        elif "qualify row_number()" in lowered:
            self._execute_latest_sent(sql)
        elif lowered.startswith("insert into"):
            self._execute_insert(sql, params)
        else:
            raise NotImplementedError(f"FakeCursor.execute cannot handle: {sql}")
        self._position = 0

    def _table(self, table_name: str) -> list[dict[str, Any]]:
        table = self._connection.tables.get(table_name)
        if table is None:
            raise RuntimeError(f"FakeCursor: no such table {table_name!r} (never created)")
        return table.rows

    def _execute_create_table(self, sql: str) -> None:
        match = _CREATE_TABLE_RE.search(sql)
        if match is None:
            raise NotImplementedError(f"FakeCursor cannot parse create-table statement: {sql}")
        table_name = match["table"]
        if table_name not in self._connection.tables:
            self._connection.tables[table_name] = stamped_table(match["flow_id"])
        # else: IF NOT EXISTS is a true no-op -- an existing table's stamp stands.

    def _execute_show_tblproperties(self, sql: str) -> None:
        table_name = sql.split("show tblproperties", 1)[1].strip()
        self._table(table_name)  # raises if the table itself does not exist
        properties = self._connection.tables[table_name].properties
        self._result_columns = ["key", "value"]
        self._result_rows = [{"key": k, "value": v} for k, v in properties.items()]

    def _execute_existence_probe(self, sql: str) -> None:
        table_name = sql.split("select 1 from", 1)[1].split("limit", 1)[0].strip()
        self._table(table_name)  # raises if missing; the (discarded) result just proves it isn't
        self._result_columns = []
        self._result_rows = []

    def _execute_select_source(self) -> None:
        rows = self._connection.source_rows
        self._result_columns = list(rows[0].keys()) if rows else []
        self._result_rows = list(rows)

    def _execute_latest_sent(self, sql: str) -> None:
        table_name = sql.split("select tracking_key, payload from", 1)[1].split("qualify", 1)[0].strip()
        rows = self._table(table_name)
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = row["tracking_key"]
            if key not in latest or row["sent_at"] >= latest[key]["sent_at"]:
                latest[key] = row
        self._result_columns = ["tracking_key", "payload"]
        self._result_rows = [
            {"tracking_key": r["tracking_key"], "payload": r["payload"]} for r in latest.values()
        ]

    def _execute_insert(self, sql: str, params: dict[str, Any]) -> None:
        """Reconstructs each row from its indexed params (`tracking_key_0`, ...): the
        real statement never inlines a value, so this fake must not assume any
        particular row count either."""
        table_name = sql.split("insert into", 1)[1].split("(", 1)[0].strip()
        rows = self._table(table_name)
        row_indices = sorted(
            {int(key.rsplit("_", 1)[1]) for key in params if key.startswith("tracking_key_")}
        )
        for i in row_indices:
            rows.append(
                {
                    "tracking_key": params[f"tracking_key_{i}"],
                    "payload": params[f"payload_{i}"],
                    "sent_at": params[f"sent_at_{i}"],
                }
            )

    @property
    def description(self) -> list[tuple[str]]:
        return [(name,) for name in self._result_columns]

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        batch = self._result_rows[self._position : self._position + size]
        self._position += len(batch)
        return [tuple(row[col] for col in self._result_columns) for row in batch]

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


@dataclass
class FakeConnection:
    """A per-table in-memory warehouse. `tables` is keyed by table name -- the fake's
    equivalent of one physical table per flow, and doubles as the exists-registry:
    a name absent from it has simply never been created. `source_rows` is
    unrelated: it is what `select * from <source_relation>` returns, since the
    source model is a different relation entirely.
    """

    source_rows: list[dict[str, Any]] = field(default_factory=list)
    tables: dict[str, FakeTable] = field(default_factory=dict)
    closed: bool = False
    execute_call_count: int = 0
    executed_sql: list[str] = field(default_factory=list)

    def cursor(self) -> FakeCursor:
        return FakeCursor(connection=self)

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeHttpTransport:
    """Returns queued responses in order and records every call it received."""

    responses: list[Any]
    calls: list[dict[str, Any]] = field(default_factory=list)

    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float) -> Any:
        self.calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return self.responses[len(self.calls) - 1]
