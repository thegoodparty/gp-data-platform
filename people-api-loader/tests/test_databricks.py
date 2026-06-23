"""core.databricks.run_statement submits to the warehouse and polls to terminal."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.core import databricks
from loader.core.config import BaseLoaderConfig

_CFG = cast(BaseLoaderConfig, SimpleNamespace(databricks_warehouse_id="wh-1"))


def _state(value: str) -> SimpleNamespace:
    """Mimic the SDK's StatementState enum: a .value-bearing object, NOT a plain string."""
    return SimpleNamespace(value=value)


class _FakeStmtApi:
    def __init__(self, states: list[str]) -> None:
        self._states = states  # successive statuses to return (as enum-like StatementState)
        self.submitted: dict | None = None

    def execute_statement(self, **kw: object) -> object:
        self.submitted = kw
        return SimpleNamespace(statement_id="s1", status=SimpleNamespace(state=_state(self._states[0])))

    def get_statement(self, statement_id: str) -> object:
        s = self._states.pop(0) if len(self._states) > 1 else self._states[0]
        return SimpleNamespace(statement_id=statement_id, status=SimpleNamespace(state=_state(s)))


def _client(api: _FakeStmtApi) -> object:
    return SimpleNamespace(statement_execution=api)


def test_state_str_normalizes_enum_and_plain_string() -> None:
    # Real SDK returns a StatementState enum (.value); tolerate a plain string too.
    assert (
        databricks._state_str(SimpleNamespace(status=SimpleNamespace(state=_state("SUCCEEDED"))))
        == "SUCCEEDED"
    )
    assert databricks._state_str(SimpleNamespace(status=SimpleNamespace(state="RUNNING"))) == "RUNNING"


def test_run_statement_polls_to_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _FakeStmtApi(["PENDING", "RUNNING", "SUCCEEDED"])
    monkeypatch.setattr(databricks, "workspace_client", lambda cfg: _client(api))
    monkeypatch.setattr(databricks, "_poll_sleep", lambda: None)
    resp = databricks.run_statement(_CFG, "SELECT 1", warehouse_id="wh-1")
    assert resp.status.state.value == "SUCCEEDED"
    assert api.submitted is not None
    assert api.submitted["warehouse_id"] == "wh-1"
    assert api.submitted["statement"] == "SELECT 1"


def test_run_statement_raises_on_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    api = _FakeStmtApi(["RUNNING", "FAILED"])
    monkeypatch.setattr(databricks, "workspace_client", lambda cfg: _client(api))
    monkeypatch.setattr(databricks, "_poll_sleep", lambda: None)
    with pytest.raises(RuntimeError, match="FAILED"):
        databricks.run_statement(_CFG, "SELECT bad", warehouse_id="wh-1")


def test_run_statement_raises_without_warehouse() -> None:
    with pytest.raises(RuntimeError, match="warehouse"):
        databricks.run_statement(_CFG, "SELECT 1", warehouse_id="")
