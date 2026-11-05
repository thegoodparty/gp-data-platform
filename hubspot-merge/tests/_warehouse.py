"""An in-memory warehouse for the two statements the log issues."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from hsmerge.log import PARAM_CHAR_LIMIT


@dataclass
class FakeWarehouse:
    """Tables keyed by name, each a column tuple and a row list."""

    columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    rows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    inserts: int = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)


class _Cursor:
    def __init__(self, warehouse: FakeWarehouse):
        self._warehouse = warehouse

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        params = params or {}
        if sum(len(str(value)) for value in params.values()) > PARAM_CHAR_LIMIT:
            raise ValueError("parameters are too large")
        if match := re.match(r"create table if not exists (\S+) \((.*)\) using delta", sql):
            table, declared = match.groups()
            if table not in self._warehouse.columns:
                self._warehouse.columns[table] = tuple(part.split()[0] for part in declared.split(", "))
                self._warehouse.rows[table] = []
        elif match := re.match(r"insert into (\S+) \(([^)]*)\)", sql):
            table, listed = match.groups()
            columns = tuple(listed.split(", "))
            if self._warehouse.columns.get(table) != columns:
                raise RuntimeError(f"{table}: columns do not match the table")
            self._warehouse.inserts += 1
            self._warehouse.rows[table].extend(json.loads(params["rows"]))
        else:
            raise NotImplementedError(sql)
