"""Unit tests for the gold-match daily DAG's helpers.

Warehouse calls run against a recording fake connection; dbt Cloud calls
against a mocked hook. Every test names the production failure it catches.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import include.custom_functions.gold_match_utils as gm

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
        "scopes": None,
    }
    with (
        patch.object(gm, "conn_kwargs", autospec=True, return_value=fields),
        patch.object(gm, "Variable", autospec=True) as mock_variable,
    ):
        variables = {
            "BRAINTRUST_API_KEY": "bt-key",
            "gold_match_aws_role_arn": "arn:aws:iam::333:role/gold-match-bedrock-prod",
            "gold_match_aws_external_id": "ext-123",
        }
        mock_variable.get.side_effect = variables.__getitem__
        env = gm.gold_match_pod_env()
    assert env == {
        "DATABRICKS_SERVER_HOSTNAME": "dbc.example",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/x",
        "DATABRICKS_CLIENT_ID": "cid",
        "DATABRICKS_CLIENT_SECRET": "sec",
        "BRAINTRUST_API_KEY": "bt-key",
        # The GoodParty-account role the pod assumes for Bedrock; its own
        # identity is Astronomer's and cannot hold the grant.
        "GOLD_MATCH_AWS_ROLE_ARN": "arn:aws:iam::333:role/gold-match-bedrock-prod",
        "GOLD_MATCH_AWS_EXTERNAL_ID": "ext-123",
        "ENVIRONMENT": "production",
    }
    # Every Variable is read through Variable.get, so an unset one fails here,
    # before the paid pod starts.
    assert {c.args[0] for c in mock_variable.get.call_args_list} == set(variables)


def test_pod_env_forwards_narrowed_scopes():
    """When the deployment's databricks_scopes Variable is set (the SP secret
    was minted narrow), the pod must request the same scopes or its token
    exchange is refused; the env forwards them as DATABRICKS_SCOPES."""
    fields = {
        "host": "dbc.example",
        "http_path": "p",
        "client_id": "cid",
        "client_secret": "sec",
        "scopes": ["sql", "unity-catalog"],
    }
    with (
        patch.object(gm, "conn_kwargs", autospec=True, return_value=fields),
        patch.object(gm, "Variable", autospec=True) as mock_variable,
    ):
        mock_variable.get.return_value = "bt"
        env = gm.gold_match_pod_env()
    assert env["DATABRICKS_SCOPES"] == "sql,unity-catalog"


def test_run_key_truncates_to_the_writers_precision():
    """attempted_at is written truncated to whole seconds; an untruncated key
    in the signal SQL matches ZERO rows while reporting nothing to signal."""
    dag_run = type("R", (), {"start_date": datetime(2026, 9, 2, 14, 30, 3, 999999, tzinfo=UTC)})
    assert gm.run_key_of(dag_run) == _RUN_KEY


def test_new_quarantine_reads_first_entries_only_and_ignores_adjudication_holds():
    """Inserts stamp first_failed_at = the run key exactly; backoff re-fails
    only re-stamp last_failed_at and must stay silent; and a hand-written
    adjudication hold stamped with the run key is the operator's own doing,
    not a pod failure the alarm should fire for."""
    cursor = _FakeCursor(rows=[(3,)])
    assert gm.new_quarantine_count(_FakeConn(cursor), _RUN_KEY) == 3
    sql, params = cursor.calls[0]
    assert "first_failed_at = :run_key" in sql
    assert "reason_code <> :adjudicated" in sql
    assert params == {"run_key": _RUN_KEY, "adjudicated": "adjudicated_wrong"}


def _hook(pages):
    hook = MagicMock()
    hook.get_job_runs.side_effect = lambda payload: MagicMock(
        json=lambda: {"data": pages[payload["job_definition_id"]]}
    )
    return hook


_NOW = datetime(2026, 9, 18, 14, 30, tzinfo=UTC)


def _run(run_id, status, cause="Triggered via schedule", finished="2026-09-18T13:47:00Z"):
    return {"id": run_id, "status": status, "trigger": {"cause": cause}, "finished_at": finished}


def test_latest_scheduled_build_succeeded_reads_the_newest_scheduled_run():
    """A hand-triggered or API-triggered run newer than the schedule (the old
    loop's rebuilds, a teammate's re-run) must not stand in for the nightly;
    the newest SCHEDULED run decides."""
    ok, label = gm.latest_scheduled_build_succeeded(
        _hook(
            {
                gm.SCHEDULED_BUILD_JOB_ID: [
                    _run(9, 20, cause="Triggered via API by x"),
                    _run(8, 10),
                    _run(7, 20),
                ]
            }
        ),
        now=_NOW,
    )
    assert ok is True and "run 8" in label and "SUCCESS" in label
    ok, label = gm.latest_scheduled_build_succeeded(
        _hook({gm.SCHEDULED_BUILD_JOB_ID: [_run(8, 30), _run(7, 10)]}),
        now=_NOW,
    )
    assert ok is False and "CANCELLED" in label


def test_latest_scheduled_build_declines_when_no_scheduled_run_is_found_and_counts_untagged_runs():
    """No scheduled run in the newest page means the universe's freshness is
    unknown: decline. Without trigger data (an API shape change) every run
    counts, so an unknown latest run that failed still declines."""
    ok, label = gm.latest_scheduled_build_succeeded(
        _hook({gm.SCHEDULED_BUILD_JOB_ID: [_run(9, 10, cause="Triggered via API")]}), now=_NOW
    )
    assert ok is False and "no scheduled run" in label
    ok, _ = gm.latest_scheduled_build_succeeded(
        _hook({gm.SCHEDULED_BUILD_JOB_ID: [{"id": 9, "status": 20}, {"id": 8, "status": 10}]}), now=_NOW
    )
    assert ok is False


def test_inflight_prod_builds_reports_live_runs_of_both_prod_jobs_only():
    """Admission must see a queued, starting or running run of either prod-
    writing job and ignore finished ones; a live run is always among the
    newest, so one page ordered by -id suffices."""
    live = gm.inflight_prod_builds(
        _hook(
            {
                gm.SCHEDULED_BUILD_JOB_ID: [_run(1, 10), _run(2, 3)],
                gm.ON_MERGE_BUILD_JOB_ID: [_run(3, 20), _run(4, 1)],
            }
        )
    )
    assert live == [
        f"job {gm.SCHEDULED_BUILD_JOB_ID} run 2 (RUNNING)",
        f"job {gm.ON_MERGE_BUILD_JOB_ID} run 4 (QUEUED)",
    ]


def test_no_dbt_trigger_or_cancel_remains_in_the_helpers():
    """The loop rides the scheduled nightly; a trigger or cancel call creeping
    back in would re-couple publication to a build the DAG owns."""
    import inspect

    source = inspect.getsource(gm)
    assert "trigger_job_run" not in source
    assert "cancel_job_run" not in source
