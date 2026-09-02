"""Unit tests for the gold-match daily DAG's helpers.

Warehouse calls run against a recording fake connection; dbt Cloud calls
against a mocked hook. Every test names the production failure it catches.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, call, patch

import include.custom_functions.gold_match_utils as gm
import pytest
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudJobRunStatus

_RUN_KEY = datetime(2026, 9, 2, 14, 30, 3, tzinfo=UTC)


class _FakeCursor:
    """Records (sql, parameters) per execute; serves canned fetchone rows."""

    def __init__(self, rows):
        self.calls = []
        self._rows = list(rows)
        self._current = None

    def execute(self, sql, parameters=None):
        self.calls.append((" ".join(sql.split()), parameters))
        self._current = self._rows.pop(0)

    def fetchone(self):
        return self._current

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_pod_env_speaks_the_clients_names():
    """The client reads DATABRICKS_SERVER_HOSTNAME (bare host); emitting
    matcha's DATABRICKS_HOST form, or a scheme-prefixed host, would leave the
    pod unable to authenticate."""
    fields = {
        "host": "https://dbc.example/",
        "http_path": "/sql/1.0/warehouses/x",
        "client_id": "cid",
        "client_secret": "sec",
    }
    with (
        patch.object(gm, "conn_kwargs", autospec=True, return_value=fields),
        patch.object(gm, "Variable", autospec=True) as mock_variable,
    ):
        mock_variable.get.return_value = "bt-key"
        env = gm.gold_match_pod_env()
    assert env == {
        "DATABRICKS_SERVER_HOSTNAME": "dbc.example",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/x",
        "DATABRICKS_CLIENT_ID": "cid",
        "DATABRICKS_CLIENT_SECRET": "sec",
        "BRAINTRUST_API_KEY": "bt-key",
        "ENVIRONMENT": "production",
    }
    mock_variable.get.assert_called_once_with("BRAINTRUST_API_KEY")


def test_pod_env_refuses_a_scoped_connection():
    """A connection with a narrower OAuth `scopes` extra authenticates the
    gate tasks but fails inside every paid pod (the client requests the SDK
    default); failing at pre_execute names the mismatch before a pod starts."""
    fields = {
        "host": "dbc.example",
        "http_path": "p",
        "client_id": "cid",
        "client_secret": "sec",
        "scopes": ["sql"],
    }
    with (
        patch.object(gm, "conn_kwargs", autospec=True, return_value=fields),
        patch.object(gm, "Variable", autospec=True),
        pytest.raises(ValueError, match="scopes"),
    ):
        gm.gold_match_pod_env()


def test_run_key_truncates_to_the_writers_precision():
    """attempted_at is written truncated to whole seconds; an untruncated key
    in the delete/gate SQL matches ZERO rows while reporting success."""
    dag_run = type("R", (), {"start_date": datetime(2026, 9, 2, 14, 30, 3, 999999, tzinfo=UTC)})
    assert gm.run_key_of(dag_run) == _RUN_KEY


def test_gate_queries_bind_the_run_key_and_map_both_metrics():
    """The run-scoped count must be scoped by attempted_at equality — an
    unscoped query would attribute every historical dead tuple to this run and
    delete a healthy day's work."""
    cursor = _FakeCursor(rows=[(2,), (5,)])
    metrics = gm.run_gate_queries(_FakeConn(cursor), _RUN_KEY)
    assert metrics == {"run_scoped_dead": 2, "global_dead": 5}
    run_sql, run_params = cursor.calls[0]
    assert "attempted_at = :run_key" in run_sql
    assert run_params == {"run_key": _RUN_KEY}
    global_sql, _ = cursor.calls[1]
    assert "stg_model_predictions__llm_l2_br_match" in global_sql
    # The 2026-01-26 baseline exclusion mirrors the staging label test.
    assert "2026-01-26" in global_sql


def test_delete_targets_only_the_runs_rows_and_reports_the_count():
    """Cleanup deletes by key with no expected_count (a pod dead mid-write has
    no recorded count); the pre-count is the operator's audit line."""
    cursor = _FakeCursor(rows=[(7,), None])
    deleted = gm.delete_run_rows(_FakeConn(cursor), _RUN_KEY)
    assert deleted == 7
    delete_sql, delete_params = cursor.calls[1]
    assert delete_sql.startswith(f"delete from {gm.RESULTS_TABLE}")
    assert "attempted_at = :run_key" in delete_sql
    assert delete_params == {"run_key": _RUN_KEY}


def test_new_quarantine_reads_first_entries_only():
    """Inserts stamp first_failed_at = the run key exactly; backoff re-fails
    only re-stamp last_failed_at and must stay silent."""
    cursor = _FakeCursor(rows=[(3,)])
    assert gm.new_quarantine_count(_FakeConn(cursor), _RUN_KEY) == 3
    sql, params = cursor.calls[0]
    assert "first_failed_at = :run_key" in sql
    assert params == {"run_key": _RUN_KEY}


def test_cancel_requires_terminal_confirmation_after_cancelling():
    """The provider's own kill path only warns on cancel/confirm failure;
    cleanup must not delete-and-rebuild while a cancelled rebuild could still
    be writing, so the wait (which raises on timeout) comes AFTER the cancel."""
    hook = MagicMock()
    gm.cancel_dbt_run_and_confirm(hook, 555)
    assert hook.mock_calls[0] == call.cancel_job_run(555)
    wait_kwargs = hook.wait_for_job_run_status.call_args.kwargs
    assert wait_kwargs["run_id"] == 555
    assert wait_kwargs["expected_statuses"] == DbtCloudJobRunStatus.TERMINAL_STATUSES.value


def test_cancel_tolerates_an_already_terminal_run():
    """On the common gates-failure path the rebuild already SUCCEEDED; an API
    objection to cancelling a terminal run must not kill cleanup before the
    delete — the terminal-confirmation wait still runs."""
    hook = MagicMock()
    hook.cancel_job_run.side_effect = RuntimeError("run already terminal")
    gm.cancel_dbt_run_and_confirm(hook, 555)
    assert hook.wait_for_job_run_status.called


def test_trigger_rebuild_waits_for_success_with_the_given_cause():
    """The repair rebuild is unconditional and must carry the operator-readable
    cause; waiting on SUCCESS makes a failed repair raise (the hook raises on
    wrong-terminal and on timeout) instead of silently passing."""
    hook = MagicMock()
    hook.trigger_job_run.return_value.json.return_value = {"data": {"id": 777}}
    run_id = gm.trigger_rebuild_and_wait(hook, cause="gold-match daily: cleanup rebuild after failed run (t)")
    assert run_id == 777
    trigger_kwargs = hook.trigger_job_run.call_args.kwargs
    assert trigger_kwargs["job_id"] == gm.GOLD_MATCH_REBUILD_JOB_ID
    assert trigger_kwargs["cause"].startswith("gold-match daily: cleanup rebuild")
    wait_kwargs = hook.wait_for_job_run_status.call_args.kwargs
    assert wait_kwargs["run_id"] == 777
    assert wait_kwargs["expected_statuses"] == DbtCloudJobRunStatus.SUCCESS.value
