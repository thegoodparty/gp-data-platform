"""Reads and appends a flow's own `sent_log`-shaped table: one table per flow.

Each flow owns its own log table -- dev/sandbox isolation is which table a flow's
own config points at, not a shared table filtered by a column. The table's identity
is carried by a `retl.flow_id` TBLPROPERTIES stamp, set at init and verified by the
run path before it trusts any row read from it: a flow whose
`RETL_FLOW_<NAME>_LOG_TABLE` points at ANOTHER flow's table would otherwise read
that table's rows as its own latest_sent -- non-empty, so the empty-log guard alone
could never catch it -- full-resend its own population, and append its rows into
the wrong table, corrupting both flows' histories at once.

Appends are INSERTs only, through `SentLogWriter`: this
module has no update/delete/merge path. With job-owned tables that is code
discipline rather than a grant, and Delta table history is the tamper-evidence.

DDL lives only in `create_log_table_sql`/`init_log_table`, and only the init
ceremony (`retl --init-log`) calls it. The run path (`read_latest_sent`,
`append_sent_log`) never creates or alters a table, so a lost table still fails
the run exactly as before.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, Self

from .databricks_io import fetch_all_rows

FLOW_ID_PROPERTY = "retl.flow_id"

# Every INSERT is its own Delta commit (~2.5s live), so each packs as many rows as
# the warehouse allows: it rejects any statement whose parameters total more than this.
PARAM_CHAR_LIMIT = 1_048_576
_CHUNK_CHAR_BUDGET = PARAM_CHAR_LIMIT - 100_000  # headroom for the stamp and the [] framing


class _Connection(Protocol):
    def cursor(self) -> Any: ...


class WrongLogTableError(RuntimeError):
    """A log table's stamped identity does not match the flow reading it.

    Reading a mismatched (or unstamped) table would treat another flow's rows as
    this flow's latest_sent -- non-empty, so the empty-log guard alone cannot catch
    it -- and appending to it would corrupt both flows' histories. Zero sends
    either way.
    """

    def __init__(self, flow_id: str, log_table: str, *, found: str | None):
        self.flow_id = flow_id
        self.log_table = log_table
        self.found = found
        super().__init__(f"flow {flow_id!r}: {log_table} is stamped for flow {found!r}, not {flow_id!r}")


def create_log_table_sql(log_table: str, flow_id: str) -> str:
    """The package's only DDL. IF NOT EXISTS only: init must be physically unable to
    wipe an existing table's history. `log_table` and `flow_id` are trusted config
    interpolated directly into the statement -- DB-API params cannot bind a table
    name or a TBLPROPERTIES value either way.
    """
    return (
        f"create table if not exists {log_table} ("
        "tracking_key string not null, "
        "payload string not null, "
        "sent_at timestamp not null"
        f") using delta cluster by (tracking_key) tblproperties ('{FLOW_ID_PROPERTY}' = '{flow_id}')"
    )


def _read_flow_id_property(connection: _Connection, log_table: str) -> str | None:
    """The table's current `retl.flow_id` stamp, or None if it was never set.

    Reads the unfiltered property list and looks the key up in Python, rather than
    the single-key `show tblproperties <table> ('key')` form: that form's exact
    result shape has not been exercised against a live warehouse, while the
    unfiltered (key, value) shape is the long-documented, unambiguous one.
    """
    with connection.cursor() as cursor:
        cursor.execute(f"show tblproperties {log_table}")
        rows = fetch_all_rows(cursor)
    properties = {row["key"]: row["value"] for row in rows}
    return properties.get(FLOW_ID_PROPERTY)


def verify_log_table_identity(connection: _Connection, log_table: str, flow_id: str) -> None:
    """One metadata query, run before any row read from `log_table` is trusted."""
    found = _read_flow_id_property(connection, log_table)
    if found != flow_id:
        raise WrongLogTableError(flow_id, log_table, found=found)


def init_log_table(connection: _Connection, log_table: str, flow_id: str) -> None:
    """Create `log_table`, stamped for `flow_id`, if it does not exist yet.

    Verifies the stamp either way: IF NOT EXISTS makes re-running init against an
    EXISTING table a true no-op, so without this check, pointing init at another
    flow's already-stamped table would silently succeed instead of failing.
    """
    with connection.cursor() as cursor:
        cursor.execute(create_log_table_sql(log_table, flow_id))
    verify_log_table_identity(connection, log_table, flow_id)


def latest_sent_sql(log_table: str) -> str:
    """The latest-per-key read, as a standalone statement.

    No flow filter: this table holds exactly one flow's rows by construction (one
    table per flow), which `verify_log_table_identity` checks separately before this
    ever runs. `log_table` is trusted config, not user input -- DB-API params cannot
    bind a table name.
    """
    return (
        f"select tracking_key, payload from {log_table} "
        "qualify row_number() over (partition by tracking_key order by sent_at desc) = 1"
    )


def read_latest_sent(connection: _Connection, *, log_table: str) -> dict[str, str]:
    """The latest logged payload per tracking_key in `log_table`."""
    with connection.cursor() as cursor:
        cursor.execute(latest_sent_sql(log_table))
        rows = fetch_all_rows(cursor)
    return {row["tracking_key"]: row["payload"] for row in rows}


def json_rows_insert_sql(table: str, columns: Sequence[str], stamp_column: str) -> str:
    """One INSERT that lands every row in its `:rows` JSON param, each stamped with `:stamp`.

    The rows travel as ONE JSON string parameter, decoded server-side by from_json. The
    connector's ArrayParameter would be the obvious shape, but a live warehouse binds a
    Python list as an empty array<void>: the insert succeeds and lands zero rows,
    silently. Values are never inlined: quote-escaping SQL by hand is a known trap in
    this repo, and json.dumps plus from_json round-trips any string byte-identically.
    """
    schema = "array<struct<" + ",".join(f"{c}:string" for c in columns) + ">>"
    return (
        f"insert into {table} ({', '.join(columns)}, {stamp_column}) "
        f"select {', '.join(f'r.{c}' for c in columns)}, :stamp "
        f"from (select explode(from_json(:rows, '{schema}')) as r)"
    )


class JsonRowsWriter:
    """Buffers rows and appends each full chunk as one INSERT the moment it fills.

    Use as a context manager: exit appends the remainder, including when the caller
    raises, so nothing already accepted is left unwritten. A hard kill loses at most one
    partial chunk. Append-only: INSERT, never update/delete/merge.
    """

    def __init__(self, connection: _Connection, sql: str, *, stamp: datetime | None = None):
        self._connection = connection
        self._sql = sql
        self._stamp = stamp or datetime.now(UTC)
        self._rows: list[str] = []
        self._size = 0

    def add_row(self, row: Mapping[str, str]) -> None:
        encoded = json.dumps(row, separators=(",", ":"))
        # A single row over budget still goes alone, and the warehouse rejects it loudly.
        if self._rows and self._size + len(encoded) + 1 > _CHUNK_CHAR_BUDGET:
            self._flush()
        self._rows.append(encoded)
        self._size += len(encoded) + 1

    def _flush(self) -> None:
        if not self._rows:
            return
        with self._connection.cursor() as cursor:
            cursor.execute(self._sql, {"rows": "[" + ",".join(self._rows) + "]", "stamp": self._stamp})
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
            # the unwritten rows are simply redone next run.
            exc.add_note(f"flush on exit also failed: {flush_exc}")


class SentLogWriter(JsonRowsWriter):
    """Appends confirmed (tracking_key, payload) rows to a flow's log table.

    Only rows a destination definitively confirmed belong here: logging an attempt would
    mark a rejected row as delivered and never retry it.
    """

    def __init__(self, connection: _Connection, log_table: str, *, sent_at: datetime | None = None):
        super().__init__(
            connection, json_rows_insert_sql(log_table, ("tracking_key", "payload"), "sent_at"), stamp=sent_at
        )

    def add(self, confirmed: Mapping[str, str]) -> None:
        for tracking_key, payload in confirmed.items():
            self.add_row({"tracking_key": tracking_key, "payload": payload})


def append_sent_log(
    connection: _Connection,
    *,
    log_table: str,
    confirmed: Mapping[str, str],
    sent_at: datetime | None = None,
) -> None:
    """Append one row per confirmed (tracking_key, payload), all stamped with one `sent_at`."""
    with SentLogWriter(connection, log_table, sent_at=sent_at) as writer:
        writer.add(confirmed)
