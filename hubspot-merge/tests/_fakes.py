"""In-memory stand-in for a Databricks connection, at the boundary the log writer talks to.

Fakes over mocks: this behaves like the warehouse for the two statement shapes the writer
issues. It parses the DDL's column list, so a column missing from the table definition
fails an insert here rather than only against a live warehouse, and it enforces the
per-statement parameter size limit, so a chunking bug fails here too.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from hsmerge.log import PARAM_CHAR_LIMIT


@dataclass
class FakeTable:
    """One in-memory table: its declared columns and its rows."""

    columns: tuple[str, ...] = ()
    rows: list[dict[str, Any]] = field(default_factory=list)


class FakeCursor:
    """Serves create-if-not-exists and the chunked JSON-array insert.

    `tables` doubles as the exists-registry: a name absent from it has never been created.
    """

    def __init__(self, *, connection: FakeConnection):
        self.connection = connection

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        params = params or {}
        # The warehouse rejects a statement whose parameters exceed this in total.
        param_size = sum(len(str(value)) for value in params.values())
        if param_size > PARAM_CHAR_LIMIT:
            raise ValueError(f"parameters too large: {param_size} > {PARAM_CHAR_LIMIT}")
        self.connection.param_sizes.append(param_size)
        self.connection.executed_sql.append(sql)
        lowered = sql.lower()
        if lowered.startswith("create table if not exists"):
            self._create_table(sql)
        elif lowered.startswith("insert into"):
            self._insert(sql, params)
        else:
            raise NotImplementedError(f"FakeCursor.execute cannot handle: {sql}")

    def _create_table(self, sql: str) -> None:
        table_name = sql.split("create table if not exists", 1)[1].split("(", 1)[0].strip()
        declared = sql.split("(", 1)[1].split(") using delta", 1)[0]
        columns = tuple(part.split()[0] for part in declared.split(","))
        # IF NOT EXISTS is a true no-op: an existing table's columns and rows stand.
        if table_name not in self.connection.tables:
            self.connection.tables[table_name] = FakeTable(columns=columns)

    def _insert(self, sql: str, params: dict[str, Any]) -> None:
        """Decodes the rows from the one JSON `rows` param, as the statement's `from_json` does."""
        table_name = sql.split("insert into", 1)[1].split("(", 1)[0].strip()
        columns = [c.strip() for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
        table = self.connection.tables.get(table_name)
        if table is None:
            raise RuntimeError(f"FakeCursor: no such table {table_name!r} (never created)")
        if set(columns) - set(table.columns):
            raise RuntimeError(f"FakeCursor: {table_name} has no columns {set(columns) - set(table.columns)}")
        self.connection.insert_call_count += 1
        for decoded in json.loads(params["rows"]):
            table.rows.append({column: decoded[column] for column in columns})

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


@dataclass
class FakeConnection:
    """A per-table in-memory warehouse, keyed by full table name."""

    tables: dict[str, FakeTable] = field(default_factory=dict)
    insert_call_count: int = 0
    param_sizes: list[int] = field(default_factory=list)
    executed_sql: list[str] = field(default_factory=list)

    def cursor(self) -> FakeCursor:
        return FakeCursor(connection=self)
