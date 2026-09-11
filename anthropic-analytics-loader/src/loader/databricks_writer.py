"""Databricks SQL warehouse access via the Statement Execution API.

Mirrors people-api-loader's `core/databricks.py` thin-client style (same
`workspace_client()` / poll-to-terminal-state shape), but implemented locally
since that harness is scoped to the RDS/S3 pipeline and importing it here
would pull in unrelated dependencies.
"""

from __future__ import annotations

import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import structlog

from loader.config import Config
from loader.tables import TableSpec

log = structlog.get_logger()

_TERMINAL_OK = {"SUCCEEDED"}
_TERMINAL_BAD = {"FAILED", "CANCELED", "CLOSED"}
_POLL_SECONDS = 3
_INSERT_BATCH_SIZE = 500


def workspace_client() -> Any:
    """A databricks WorkspaceClient (auth from standard Databricks env/config)."""
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _state_str(resp: Any) -> str:
    state = resp.status.state
    return getattr(state, "value", state)


def _error_detail(resp: Any) -> str:
    err = getattr(getattr(resp, "status", None), "error", None)
    if err is None:
        return ""
    code = getattr(getattr(err, "error_code", None), "value", getattr(err, "error_code", "")) or ""
    message = getattr(err, "message", "") or ""
    return f"{code}: {message}".strip(": ").strip()


def run_statement(warehouse_id: str, statement: str) -> Any:
    if not warehouse_id:
        raise RuntimeError("no warehouse_id -- set LOADER_DATABRICKS_WAREHOUSE_ID.")
    api = workspace_client().statement_execution
    resp = api.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="0s")
    statement_id = resp.statement_id
    state = _state_str(resp)
    while state not in _TERMINAL_OK and state not in _TERMINAL_BAD:
        time.sleep(_POLL_SECONDS)
        resp = api.get_statement(statement_id)
        state = _state_str(resp)
    if state in _TERMINAL_BAD:
        detail = _error_detail(resp)
        reason = f" -- {detail}" if detail else ""
        raise RuntimeError(
            f"Databricks statement {statement_id} ended {state}{reason}. SQL: {statement[:300]}"
        )
    return resp


def query_rows(warehouse_id: str, statement: str) -> list[list[Any]]:
    """Run a SELECT and return its rows as a list of value lists (columns in SELECT order)."""
    resp = run_statement(warehouse_id, statement)
    if resp.result is None or resp.result.data_array is None:
        return []
    return list(resp.result.data_array)


def sql_literal(value: Any) -> str:
    """A safe SQL literal for `value`. No string is ever interpolated unescaped."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, Decimal):
        # format(..., "f") avoids scientific notation, which repr()/str() can use for very
        # small/large Decimals and which isn't a valid numeric SQL literal.
        return format(value, "f")
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat()}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def ensure_schema(cfg: Config) -> None:
    run_statement(cfg.databricks_warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")


def ensure_table(cfg: Config, spec: TableSpec) -> None:
    run_statement(cfg.databricks_warehouse_id, spec.ddl(cfg.catalog, cfg.schema))


def replace_range(cfg: Config, spec: TableSpec, rows: list[dict], range_start: Any, range_end: Any) -> int:
    """Idempotent resync: delete `[range_start, range_end)` on `spec.range_column`, then insert `rows`."""
    full_table = f"{cfg.catalog}.{cfg.schema}.{spec.name}"
    run_statement(
        cfg.databricks_warehouse_id,
        f"DELETE FROM {full_table} WHERE {spec.range_column} >= {sql_literal(range_start)} "
        f"AND {spec.range_column} < {sql_literal(range_end)}",
    )
    if not rows:
        return 0
    cols = spec.column_names
    col_list = ", ".join(cols)
    for i in range(0, len(rows), _INSERT_BATCH_SIZE):
        batch = rows[i : i + _INSERT_BATCH_SIZE]
        values_sql = ",\n".join("(" + ", ".join(sql_literal(row.get(c)) for c in cols) + ")" for row in batch)
        run_statement(
            cfg.databricks_warehouse_id,
            f"INSERT INTO {full_table} ({col_list}) VALUES\n{values_sql}",
        )
        log.info("inserted_batch", table=spec.name, batch_size=len(batch))
    return len(rows)
