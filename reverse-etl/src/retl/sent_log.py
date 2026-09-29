"""Reads and appends a flow's own `sent_log`-shaped table: one table per flow.

Each flow owns its own log table -- dev/sandbox isolation is which table a flow's
own config points at, not a shared table filtered by a column. The table's identity
is carried by a `retl.flow_id` TBLPROPERTIES stamp, set at init and verified by the
run path before it trusts any row read from it: a flow whose
`RETL_FLOW_<NAME>_LOG_TABLE` points at ANOTHER flow's table would otherwise read
that table's rows as its own latest_sent -- non-empty, so the empty-log guard alone
could never catch it -- full-resend its own population, and append its rows into
the wrong table, corrupting both flows' histories at once.

Appends are INSERTs only, one call per flush of confirmed rows (see run.py): this
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
from typing import Any, Protocol

from .databricks_io import fetch_all_rows

FLOW_ID_PROPERTY = "retl.flow_id"

# Every INSERT is a sequential round trip and its own Delta commit (~2.5s apiece
# against a live warehouse), so the append packs as many rows into one statement as
# the warehouse allows. It caps a statement's parameters at this many characters in
# total and rejects anything larger outright.
PARAM_CHAR_LIMIT = 1_048_576
# Headroom under the limit for the sent_at param and anything the size estimate misses.
_CHUNK_CHAR_BUDGET = 900_000

# The rows travel as ONE JSON string parameter, decoded server-side by from_json. The
# connector's ArrayParameter would be the obvious shape, but a live warehouse binds a
# Python list as an empty array<void>: the insert succeeds and lands zero rows, silently.
_ROWS_SCHEMA = "array<struct<tracking_key:string,payload:string>>"


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


def insert_sql(log_table: str) -> str:
    """One INSERT that lands every row in its `:rows` JSON param, stamped with `:sent_at`.

    Values are never inlined: quote-escaping SQL by hand is a known trap in this repo,
    and json.dumps plus from_json round-trips any payload byte-identically.
    """
    return (
        f"insert into {log_table} (tracking_key, payload, sent_at) "
        "select r.tracking_key, r.payload, :sent_at "
        f"from (select explode(from_json(:rows, '{_ROWS_SCHEMA}')) as r)"
    )


def _chunks_under_budget(items: Sequence[tuple[str, str]]) -> list[str]:
    """Split `items` into JSON-encoded row arrays of at most `_CHUNK_CHAR_BUDGET` characters.

    A single row larger than the budget still goes alone; the warehouse then rejects
    it loudly, which is the right outcome for a payload that size.
    """
    chunks: list[str] = []
    current: list[str] = []
    size = 2  # the enclosing []
    for tracking_key, payload in items:
        encoded = json.dumps({"tracking_key": tracking_key, "payload": payload})
        if current and size + len(encoded) + 1 > _CHUNK_CHAR_BUDGET:
            chunks.append("[" + ",".join(current) + "]")
            current, size = [], 2
        current.append(encoded)
        size += len(encoded) + 1
    if current:
        chunks.append("[" + ",".join(current) + "]")
    return chunks


def append_sent_log(
    connection: _Connection,
    *,
    log_table: str,
    confirmed: Mapping[str, str],
    sent_at: datetime | None = None,
) -> None:
    """Append one row per confirmed (tracking_key, payload), stamped with one `sent_at`.

    Only rows the destination definitively confirmed belong here: logging an attempt
    would mark a rejected row as delivered and never retry it, and logging before
    delivery would suppress a failed send's person forever.

    Packed into as few INSERTs as the parameter size limit allows rather than one
    `execute` per row: this is still append-only (INSERT, never update/delete/merge)
    and still one `sent_at` stamp for the whole call.
    """
    if not confirmed:
        return
    stamp = sent_at or datetime.now(UTC)
    sql = insert_sql(log_table)
    with connection.cursor() as cursor:
        for rows_json in _chunks_under_budget(list(confirmed.items())):
            cursor.execute(sql, {"rows": rows_json, "sent_at": stamp})
