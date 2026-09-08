"""Shared fakes: in-memory stand-ins for a Databricks connection and an HTTP transport.

Fakes over mocks (ai-rules/test-engineer.md #6): these behave like the real thing at
the boundary retl talks to, so a test failure means retl's own logic is wrong, not
that a mock's script drifted from what the real dependency does.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class FakeCursor:
    """Serves three statement shapes: a flat source `select *`, sent_log's
    latest-per-key read, and sent_log's chunked multi-row insert.

    `log_table` is a shared mutable list simulating the sent_log rows currently
    persisted; an insert appends to it, so append-then-read round trips work across
    cursors from the same FakeConnection. `connection` is the same FakeConnection
    every cursor from it shares, so `execute_call_count` accumulates across the
    `with connection.cursor() as cursor:` blocks a chunked append opens one cursor
    for but calls `execute` on repeatedly.
    """

    def __init__(
        self, *, source_rows: list[dict[str, Any]], log_table: list[dict[str, Any]], connection: Any
    ):
        self._source_rows = source_rows
        self._log_table = log_table
        self._connection = connection
        self._result_columns: list[str] = []
        self._result_rows: list[dict[str, Any]] = []
        self._position = 0
        self.arraysize = 100_000

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        params = params or {}
        self._connection.execute_call_count += 1
        lowered = sql.lower()
        if "qualify row_number()" in lowered:
            self._execute_latest_sent(params["flow_id"])
        elif lowered.startswith("select * from"):
            self._result_columns = list(self._source_rows[0].keys()) if self._source_rows else []
            self._result_rows = list(self._source_rows)
        elif lowered.startswith("insert into"):
            self._execute_insert(params)
        else:
            raise NotImplementedError(f"FakeCursor.execute cannot handle: {sql}")
        self._position = 0

    def _execute_latest_sent(self, flow_id: str) -> None:
        latest: dict[str, dict[str, Any]] = {}
        for row in self._log_table:
            if row["flow_id"] != flow_id:
                continue
            key = row["tracking_key"]
            if key not in latest or row["sent_at"] >= latest[key]["sent_at"]:
                latest[key] = row
        self._result_columns = ["tracking_key", "payload"]
        self._result_rows = [
            {"tracking_key": r["tracking_key"], "payload": r["payload"]} for r in latest.values()
        ]

    def _execute_insert(self, params: dict[str, Any]) -> None:
        """Reconstructs each row from its indexed params (`flow_id_0`, `flow_id_1`, ...):
        the real statement never inlines a value, so this fake must not assume any
        particular row count either."""
        row_indices = sorted({int(key.rsplit("_", 1)[1]) for key in params if key.startswith("flow_id_")})
        for i in row_indices:
            self._log_table.append(
                {
                    "flow_id": params[f"flow_id_{i}"],
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
    source_rows: list[dict[str, Any]] = field(default_factory=list)
    log_table: list[dict[str, Any]] = field(default_factory=list)
    closed: bool = False
    execute_call_count: int = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(source_rows=self.source_rows, log_table=self.log_table, connection=self)

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
