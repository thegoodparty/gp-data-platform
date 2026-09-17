"""Unit tests for the gold-match daily DAG's helpers.

Warehouse calls run against a recording fake connection; dbt Cloud calls
against a mocked hook. Every test names the production failure it catches.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, call, patch

import include.custom_functions.gold_match_utils as gm
import pytest
from airflow.exceptions import AirflowException
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
    # The baseline exclusion mirrors the staging label test, with the UTC
    # offset pinned (a bare literal reads in the session timezone).
    assert "timestamp'2026-01-26 00:00:00+00:00'" in global_sql


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
    only re-stamp last_failed_at and must stay silent; and a hand-written
    adjudication hold stamped with the run key is the operator's own doing,
    not a pod failure the alarm should fire for."""
    cursor = _FakeCursor(rows=[(3,)])
    assert gm.new_quarantine_count(_FakeConn(cursor), _RUN_KEY) == 3
    sql, params = cursor.calls[0]
    assert "first_failed_at = :run_key" in sql
    assert "reason_code <> :adjudicated" in sql
    assert params == {"run_key": _RUN_KEY, "adjudicated": "adjudicated_wrong"}


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


def _clean_results(**overrides):
    """run_results.json with every listed model built and every listed test
    passed (the generic test carrying its real hash suffix), plus an
    unrelated failing test elsewhere in the project."""
    results = [{"unique_id": uid, "status": "success"} for uid in gm.MATCHER_DEPENDENT_MODELS]
    for uid in gm.MATCHER_RELEVANT_TESTS:
        # generic tests carry dbt's hash suffix after the argument-derived name
        generic = "tuple_exists" in uid or "not_null_" in uid or "unique_" in uid
        results.append(
            {
                "unique_id": uid + (".0c4b2ae075" if generic else ""),
                "status": "warn" if "tuple_exists" in uid else "pass",
            }
        )
    results.append(
        {
            "unique_id": "test.goodparty_data_catalog.not_null_stg_airbyte_source__gp_api_db_outreach_campaignId.1",
            "status": "fail",
        }
    )
    for uid, status in overrides.items():
        for r in results:
            if r["unique_id"].startswith(uid):
                r["status"] = status
    return {"results": results}


def test_trigger_rebuild_turns_docs_off_and_carries_the_cause():
    """The docs step re-entering the loop's runs is how three builds died at
    the plan's memory cap after completing; both trigger paths go through
    here, so one assertion covers the rebuild and the repair."""
    hook = MagicMock()
    hook.trigger_job_run.return_value.json.return_value = {"data": {"id": 777}}
    assert gm.trigger_rebuild(hook, cause="gold-match daily: post-write rebuild (run t)") == 777
    kwargs = hook.trigger_job_run.call_args.kwargs
    assert kwargs["job_id"] == gm.GOLD_MATCH_REBUILD_JOB_ID
    assert kwargs["cause"].startswith("gold-match daily: post-write rebuild")
    assert kwargs["additional_run_config"] == {"generate_docs_override": False}


def test_result_problems_pass_an_unrelated_red_and_fail_a_skipped_model_or_failed_test():
    """A full-project build is red whenever ANY test fails; the loop must
    publish when its own lineage is clean and must NOT when a matcher model
    was skipped or a matcher-relevant test failed, whatever the run status."""
    assert gm.rebuild_result_problems(_clean_results()) == []
    skipped = gm.rebuild_result_problems(
        _clean_results(**{"model.goodparty_data_catalog.m_election_api__position": "skipped"})
    )
    assert skipped and "m_election_api__position" in skipped[0]
    failed = gm.rebuild_result_problems(
        _clean_results(
            **{"test.goodparty_data_catalog.assert_position_district_voter_coverage_floor": "fail"}
        )
    )
    assert failed and "assert_position_district_voter_coverage_floor" in failed[0]


def test_result_problems_treat_an_absent_listed_node_as_not_built():
    """A narrowed selection (or a renamed model) that drops a listed node
    must fail loudly rather than pass by omission."""
    results = _clean_results()
    results["results"] = [r for r in results["results"] if "pending_offices" not in r["unique_id"]]
    problems = gm.rebuild_result_problems(results)
    assert any("int__l2_br_match_pending_offices" in p and "not in the build" in p for p in problems)


def test_build_step_results_come_from_the_dbt_build_step_not_the_last_step():
    """dbt Cloud serves artifacts for the LAST step by default; the docs step
    (or a freshness step) after the build has no build results, so the step
    index must come from the run's own step list."""
    hook = MagicMock()
    hook.get_job_run.return_value.json.return_value = {
        "data": {
            "run_steps": [
                {"index": 5, "name": "Invoke dbt with `dbt seed`"},
                {"index": 6, "name": "Invoke dbt with `dbt build`"},
                {"index": 7, "name": "Generation of docs"},
            ]
        }
    }
    hook.get_job_run_artifact.return_value.json.return_value = _clean_results()
    gm.build_step_run_results(hook, 555)
    assert hook.get_job_run_artifact.call_args.kwargs["step"] == 6
    assert hook.get_job_run_artifact.call_args.kwargs["path"] == "run_results.json"


def test_build_step_results_probe_steps_when_the_step_list_is_unavailable():
    """If the API does not return run_steps, the last step's artifact may be
    a docs or freshness step with no build results; the lookup must probe
    step indices until it finds the matcher lineage rather than fail or,
    worse, pass on an empty artifact."""
    hook = MagicMock()
    hook.get_job_run.side_effect = RuntimeError("no run_steps")
    by_step = {None: {"results": []}, 1: RuntimeError("404"), 2: _clean_results()}

    def artifact(run_id, path, step):
        value = by_step.get(step, RuntimeError("404"))
        if isinstance(value, Exception):
            raise value
        return MagicMock(json=lambda: value)

    hook.get_job_run_artifact.side_effect = artifact
    assert gm.build_step_run_results(hook, 555) == _clean_results()
    assert [c.kwargs["step"] for c in hook.get_job_run_artifact.call_args_list] == [None, 1, 2]


def test_build_step_results_fail_loud_when_no_lineage_results_are_readable():
    """An unreadable rebuild must be treated like a failed one, never like a
    passed one: no step's artifact carrying a matcher model is not evidence
    of a build."""
    hook = MagicMock()
    hook.get_job_run.side_effect = RuntimeError("no run_steps")
    hook.get_job_run_artifact.return_value.json.return_value = {
        "results": [{"unique_id": "model.x.y", "status": "success"}]
    }
    with pytest.raises(AirflowException, match="no build results"):
        gm.build_step_run_results(hook, 555)


def test_wait_for_rebuild_judges_by_results_not_status():
    """The wait accepts ANY terminal status (the hook raises only on timeout);
    an ERROR run that is clean on the lineage passes and a SUCCESS run that
    skipped a matcher model fails."""
    hook = MagicMock()
    hook.get_job_run_status.return_value = DbtCloudJobRunStatus.ERROR.value
    with patch.object(gm, "build_step_run_results", autospec=True, return_value=_clean_results()):
        gm.wait_for_rebuild(hook, 555, timeout_s=60)
    wait_kwargs = hook.wait_for_job_run_status.call_args.kwargs
    assert wait_kwargs["expected_statuses"] == DbtCloudJobRunStatus.TERMINAL_STATUSES.value
    assert wait_kwargs["timeout"] == 60
    hook.get_job_run_status.return_value = DbtCloudJobRunStatus.SUCCESS.value
    bad = _clean_results(**{"model.goodparty_data_catalog.int__icp_offices": "skipped"})
    with (
        patch.object(gm, "build_step_run_results", autospec=True, return_value=bad),
        pytest.raises(AirflowException, match="not good for publication.*int__icp_offices"),
    ):
        gm.wait_for_rebuild(hook, 555, timeout_s=60)


def test_trigger_rebuild_and_wait_is_the_repair_path_with_the_same_criterion():
    """The repair rebuild is unconditional, carries the operator-readable
    cause, and is judged like the post-write rebuild, so an unrelated red
    never reads as a failed repair."""
    hook = MagicMock()
    hook.trigger_job_run.return_value.json.return_value = {"data": {"id": 777}}
    hook.get_job_run_status.return_value = DbtCloudJobRunStatus.ERROR.value
    with patch.object(gm, "build_step_run_results", autospec=True, return_value=_clean_results()):
        run_id = gm.trigger_rebuild_and_wait(
            hook, cause="gold-match daily: cleanup rebuild after failed run (t)"
        )
    assert run_id == 777
    assert hook.trigger_job_run.call_args.kwargs["cause"].startswith("gold-match daily: cleanup rebuild")
    assert hook.wait_for_job_run_status.call_args.kwargs["run_id"] == 777


def test_inflight_prod_builds_reports_live_runs_of_both_prod_jobs_only():
    """Admission must see a queued or running run of either prod-writing job
    and ignore finished ones; a live run is always among the newest, so one
    page ordered by -id suffices."""
    hook = MagicMock()
    pages = {
        gm.GOLD_MATCH_REBUILD_JOB_ID: [{"id": 1, "status": 10}, {"id": 2, "status": 3}],
        gm.ON_MERGE_BUILD_JOB_ID: [{"id": 3, "status": 20}, {"id": 4, "status": 1}],
    }
    hook.get_job_runs.side_effect = lambda payload: MagicMock(
        json=lambda: {"data": pages[payload["job_definition_id"]]}
    )
    live = gm.inflight_prod_builds(hook)
    assert live == [
        f"job {gm.GOLD_MATCH_REBUILD_JOB_ID} run 2 (RUNNING)",
        f"job {gm.ON_MERGE_BUILD_JOB_ID} run 4 (QUEUED)",
    ]
    assert {c.kwargs["payload"]["order_by"] for c in hook.get_job_runs.call_args_list} == {"-id"}


def test_next_sync_deadline_is_todays_sync_until_it_passes():
    """A run before 22:00Z is measured against today's sync; one after it
    against tomorrow's, or a late run would be declined forever."""
    assert gm.next_sync_deadline(datetime(2026, 9, 17, 14, 30, tzinfo=UTC)) == datetime(
        2026, 9, 17, 22, 0, tzinfo=UTC
    )
    assert gm.next_sync_deadline(datetime(2026, 9, 17, 23, 0, tzinfo=UTC)) == datetime(
        2026, 9, 18, 22, 0, tzinfo=UTC
    )
