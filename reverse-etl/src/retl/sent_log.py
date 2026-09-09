"""Reads and appends a flow's own `sent_log`-shaped table: one table per flow.

Each flow owns its own log table -- dev/sandbox isolation is which table a flow's
own config points at, not a shared table filtered by a column. The table's identity
is carried by a `retl.flow_id` TBLPROPERTIES stamp, set at init and verified by the
run path before it trusts any row read from it: a flow whose
`RETL_FLOW_<NAME>_LOG_TABLE` points at ANOTHER flow's table would otherwise read
that table's rows as its own latest_sent -- non-empty, so the empty-log guard alone
could never catch it -- full-resend its own population, and append its rows into
the wrong table, corrupting both flows' histories at once.

Appends are INSERTs only, one call per confirmed batch (never end-of-run): this
module has no update/delete/merge path. With job-owned tables that is code
discipline rather than a grant, and Delta table history is the tamper-evidence.

DDL lives only in `create_log_table_sql`/`init_log_table`, and only the init
ceremony (`retl --init-log`) calls it. The run path (`read_latest_sent`,
`append_sent_log`) never creates or alters a table, so a lost table still fails
the run exactly as before.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from .databricks_io import fetch_all_rows

FLOW_ID_PROPERTY = "retl.flow_id"

# The connector's own Cursor.executemany docstring says it issues one sequential
# request per row with no batching, so appending confirmed rows one at a time would
# be ~74k single-row round trips (and tiny Delta commits) on a first convergence, and
# again on any viability-recompute day. A chunk of 50 rows x 3 params/row = 150 bind
# markers, safely under Databricks' 255-parameter-marker statement limit.
_APPEND_CHUNK_SIZE = 50


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


def _table_exists(connection: _Connection, log_table: str) -> bool:
    """A lightweight existence probe, for init's "created vs already present" report only.

    Broad on purpose: any failure to read from the table -- not only a documented
    "table not found" error -- counts as "did not exist yet". Safe to be this loose
    because it only shapes a cosmetic report line; the DDL that follows is
    unconditionally idempotent regardless of what this probe concluded.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"select 1 from {log_table} limit 0")
    except Exception:
        return False
    return True


def init_log_table(connection: _Connection, log_table: str, flow_id: str) -> bool:
    """Create `log_table`, stamped for `flow_id`, if it does not exist yet.

    Verifies the stamp either way: IF NOT EXISTS makes re-running init against an
    EXISTING table a true no-op, so without this check, pointing init at another
    flow's already-stamped table would silently succeed instead of failing.
    Returns True if this call created the table, False if it already existed.
    """
    already_existed = _table_exists(connection, log_table)
    with connection.cursor() as cursor:
        cursor.execute(create_log_table_sql(log_table, flow_id))
    verify_log_table_identity(connection, log_table, flow_id)
    return not already_existed


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


def _chunked_insert_statement(
    log_table: str, chunk: Sequence[tuple[str, str]], sent_at: datetime
) -> tuple[str, dict[str, Any]]:
    """One multi-row INSERT for up to `_APPEND_CHUNK_SIZE` rows, every value bound by name.

    Values are never inlined: quote-escaping SQL by hand is a known trap in this
    repo, so every row's values get their own indexed named parameter
    (`:tracking_key_0`, `:tracking_key_1`, ...) instead.
    """
    value_clauses = []
    params: dict[str, Any] = {}
    for i, (tracking_key, payload) in enumerate(chunk):
        value_clauses.append(f"(:tracking_key_{i}, :payload_{i}, :sent_at_{i})")
        params[f"tracking_key_{i}"] = tracking_key
        params[f"payload_{i}"] = payload
        params[f"sent_at_{i}"] = sent_at
    sql = f"insert into {log_table} (tracking_key, payload, sent_at) values " + ", ".join(value_clauses)
    return sql, params


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

    Chunked into multi-row INSERTs of `_APPEND_CHUNK_SIZE` rows apiece rather than
    one `execute` per row: this is still append-only (INSERT, never
    update/delete/merge) and still one `sent_at` stamp for the whole call.
    """
    if not confirmed:
        return
    stamp = sent_at or datetime.now(UTC)
    items = list(confirmed.items())
    with connection.cursor() as cursor:
        for start in range(0, len(items), _APPEND_CHUNK_SIZE):
            chunk = items[start : start + _APPEND_CHUNK_SIZE]
            sql, params = _chunked_insert_statement(log_table, chunk, stamp)
            cursor.execute(sql, params)
