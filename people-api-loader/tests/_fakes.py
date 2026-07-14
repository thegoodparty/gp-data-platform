"""Fake psycopg connection/cursor for step unit tests.

Records every executed statement and serves canned results queued via
`queue_result`. Works as a context manager (the `with connect_new(...) as
conn` form) and yields cursors that are themselves context managers.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self._conn.executed.append((" ".join(sql.split()), params))

    def fetchone(self) -> Any:
        return self._conn.next_result()

    def fetchall(self) -> Any:
        res = self._conn.next_result()
        return res if res is not None else []


class FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self._results: list[Any] = []

    def queue_result(self, value: Any) -> FakeConn:
        """Queue a value to be returned by the next fetchone/fetchall."""
        self._results.append(value)
        return self

    def next_result(self) -> Any:
        return self._results.pop(0) if self._results else None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def __enter__(self) -> FakeConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def fake_connect(conn: object):
    """Return a connect_* replacement that always yields `conn`.

    `conn` is typed `object` so tests can install this for connect_new/connect_prod (a FakeConn),
    for open_new_tunnel (None, the no-bastion forward), or a real psycopg connection.
    """

    @contextmanager
    def _connect(*args: object, **kwargs: object) -> Iterator[object]:
        yield conn

    return _connect


def executed_sql(conn: FakeConn) -> list[str]:
    return [sql for sql, _ in conn.executed]
