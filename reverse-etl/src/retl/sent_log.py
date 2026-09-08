"""Reads and appends `reverse_etl.sent_log` rows, always scoped to one flow_id.

Log identity is (flow_id, tracking_key) everywhere, so every function here filters
or writes with flow_id explicitly. One flow must never read, suppress, or
invalidate another flow's rows.

Appends are INSERTs only, one call per confirmed batch (never end-of-run): this
module has no update/delete/merge path, matching the append-only grant the service
principal holds.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from .databricks_io import fetch_all_rows

# The connector's own Cursor.executemany docstring says it issues one sequential
# request per row with no batching, so appending confirmed rows one at a time would
# be ~74k single-row round trips (and tiny Delta commits) on a first convergence, and
# again on any viability-recompute day. A chunk of 50 rows x 4 params/row = 200 bind
# markers, safely under Databricks' 255-parameter-marker statement limit.
_APPEND_CHUNK_SIZE = 50


class _Connection(Protocol):
    def cursor(self) -> Any: ...


def latest_sent_sql(log_table: str) -> str:
    """The latest-per-key read, as a standalone statement.

    `flow_id` is a bind parameter (`:flow_id`), not interpolated, so this stays a
    plain parameterized statement. `log_table` is trusted config, not user input --
    DB-API params cannot bind a table name either way.
    """
    return (
        f"select tracking_key, payload from {log_table} "
        "where flow_id = :flow_id "
        "qualify row_number() over (partition by tracking_key order by sent_at desc) = 1"
    )


def read_latest_sent(connection: _Connection, *, log_table: str, flow_id: str) -> dict[str, str]:
    """The latest logged payload per tracking_key, for `flow_id` only."""
    with connection.cursor() as cursor:
        cursor.execute(latest_sent_sql(log_table), {"flow_id": flow_id})
        rows = fetch_all_rows(cursor)
    return {row["tracking_key"]: row["payload"] for row in rows}


def _chunked_insert_statement(
    log_table: str, flow_id: str, chunk: Sequence[tuple[str, str]], sent_at: datetime
) -> tuple[str, dict[str, Any]]:
    """One multi-row INSERT for up to `_APPEND_CHUNK_SIZE` rows, every value bound by name.

    Values are never inlined: quote-escaping SQL by hand is a known trap in this
    repo, so every row's values get their own indexed named parameter
    (`:tracking_key_0`, `:tracking_key_1`, ...) instead.
    """
    value_clauses = []
    params: dict[str, Any] = {}
    for i, (tracking_key, payload) in enumerate(chunk):
        value_clauses.append(f"(:flow_id_{i}, :tracking_key_{i}, :payload_{i}, :sent_at_{i})")
        params[f"flow_id_{i}"] = flow_id
        params[f"tracking_key_{i}"] = tracking_key
        params[f"payload_{i}"] = payload
        params[f"sent_at_{i}"] = sent_at
    sql = f"insert into {log_table} (flow_id, tracking_key, payload, sent_at) values " + ", ".join(
        value_clauses
    )
    return sql, params


def append_sent_log(
    connection: _Connection,
    *,
    log_table: str,
    flow_id: str,
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
            sql, params = _chunked_insert_statement(log_table, flow_id, chunk, stamp)
            cursor.execute(sql, params)
