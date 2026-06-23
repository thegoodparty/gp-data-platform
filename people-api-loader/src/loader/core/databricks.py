"""Thin databricks-sdk helpers (statement execution against a SQL warehouse).

Mirrors core/aws.py's thin-client style. The SDK is imported lazily so unit tests that patch
`workspace_client` don't require the SDK or live credentials.
"""

from __future__ import annotations

import time
from typing import Any

from loader.core.config import BaseLoaderConfig

_TERMINAL_OK = {"SUCCEEDED"}
_TERMINAL_BAD = {"FAILED", "CANCELED", "CLOSED"}
_POLL_SECONDS = 5


def workspace_client(cfg: BaseLoaderConfig) -> Any:
    """A databricks WorkspaceClient (auth from standard databricks env/config)."""
    del cfg  # auth is ambient; cfg reserved for a future explicit-host path
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _poll_sleep() -> None:
    time.sleep(_POLL_SECONDS)


def _state_str(resp: Any) -> str:
    """The statement state as a plain string.

    The SDK returns `status.state` as a `StatementState` enum, not a str, so comparing it to
    the string terminal-state sets would never match (the poll loop would hang). Normalize via
    `.value`; tolerate a plain string too (older SDKs / the test fakes).
    """
    state = resp.status.state
    return getattr(state, "value", state)


def run_statement(cfg: BaseLoaderConfig, statement: str, *, warehouse_id: str) -> Any:
    """Submit `statement` to `warehouse_id` and poll to a terminal state.

    `warehouse_id` is passed in (not read off cfg) so this core helper stays consumer-agnostic
    — per CLAUDE.md, core/ must not depend on people-api-specific config fields. Raises if no
    warehouse is given or the statement ends FAILED/CANCELED/CLOSED.
    """
    if not warehouse_id:
        raise RuntimeError("no warehouse_id — set LOADER_DATABRICKS_WAREHOUSE_ID.")
    api = workspace_client(cfg).statement_execution
    resp = api.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="0s")
    statement_id = resp.statement_id
    state = _state_str(resp)
    while state not in _TERMINAL_OK and state not in _TERMINAL_BAD:
        _poll_sleep()
        resp = api.get_statement(statement_id)
        state = _state_str(resp)
    if state in _TERMINAL_BAD:
        raise RuntimeError(f"Databricks statement {statement_id} ended {state}: {statement[:120]}")
    return resp
