"""Structure assertions for the gold-match daily DAG.

Loaded from the file path directly rather than the configured dags_folder,
matching test_matcha_er_dag.py: CI does not point dags_folder at astro/dags,
and building the DagBag at collection time keeps this on real Airflow with no
metastore dependency.
"""

import logging
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "gold_match_daily.py")
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("gold_match_daily")


def test_dag_loads():
    assert _DAG is not None, f"gold_match_daily failed to load from {_DAG_FILE}"


def test_daily_after_the_universe_moves_and_paused_on_creation():
    """14:30 UTC sits after the 08:00 L2 load and the 12:02 build with hours of
    margin before the 00:00 sync — the scheduling contract IS the sync-race
    answer, so the cron must not drift. Paused on creation because BUILD must
    not schedule anything; unpausing is the owner-gated activation."""
    assert _DAG.timetable.expression == "30 14 * * *"
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_one_writer_per_run_key():
    """A manual trigger during a scheduled run must queue, not start a second
    writer against the warehouse."""
    assert _DAG.max_active_runs == 1


def test_retries_meet_the_repo_floor():
    """test_dag_example.py::test_dag_retries asserts >= 2 for every DAG."""
    assert _DAG.default_args["retries"] >= 2


def test_single_attempt_tasks():
    """match_pod: the daily retry is tomorrow's run — a same-key retry reopens
    the resume/overlap states the design removed. rebuild: the schedule
    geometry assumes ONE ~2h attempt; inherited retries would push cleanup past
    the ~22:00 operational rule. cleanup_finalizer: the default retry policy
    must not loop the ~100-minute repair cycle toward midnight."""
    for task_id in ("match_pod", "rebuild", "cleanup_finalizer"):
        assert _DAG.get_task(task_id).retries == 0, task_id


def test_cleanup_fires_on_any_pipeline_failure_and_only_those():
    """one_failed fires on a failed OR upstream_failed direct upstream
    (verified in the installed scheduler), so the trigger-set membership is
    the whole contract: all three pipeline tasks are direct upstreams, and
    operator_signal must NOT be one, or a deliberate signal failure could
    delete gated results."""
    cleanup = _DAG.get_task("cleanup_finalizer")
    assert cleanup.trigger_rule == "one_failed"
    assert {t.task_id for t in cleanup.upstream_list} == {"match_pod", "rebuild", "gates"}


def test_signal_runs_regardless_and_cannot_trigger_cleanup():
    """all_done off match_pod AND gates: a first quarantine still signals when
    the rebuild or gates fail afterward. Outside the cleanup trigger set, its
    deliberate failure can never destroy a run."""
    signal = _DAG.get_task("operator_signal")
    assert signal.trigger_rule == "all_done"
    assert {t.task_id for t in signal.upstream_list} == {"match_pod", "gates"}
    assert "cleanup_finalizer" not in {t.task_id for t in signal.downstream_list}


def test_pipeline_order():
    assert {t.task_id for t in _DAG.get_task("rebuild").upstream_list} == {"match_pod"}
    assert {t.task_id for t in _DAG.get_task("gates").upstream_list} == {"rebuild"}


def test_pod_runs_the_daily_module_with_the_dagrun_key():
    """The image entrypoint is `python -m`, so args[0] must be the module; the
    run key is the DagRun's own timestamp templated in — no minting task, no
    XCom, and cleanup can always re-derive the same key."""
    args = _DAG.get_task("match_pod").arguments
    assert args[0] == "stitch_golden_data.prod_gold_data.daily_run"
    assert args[args.index("--run-key") + 1] == "{{ dag_run.start_date }}"


def test_pod_image_requires_the_tag_variable_with_no_mutable_default():
    """An unattended loop with a `latest` default silently runs whatever main
    last published after every merge — the evaluated artifact must be the
    production artifact, so the Variable (the gate-passed sha) is required and
    an unset value fails at render, before any pod runs. Always because
    Kubernetes otherwise infers pull policy FROM the tag."""
    pod = _DAG.get_task("match_pod")
    assert "var.value.gold_match_image_tag" in pod.image
    assert "latest" not in pod.image
    assert pod.image.startswith("ghcr.io/thegoodparty/gp-data-platform/gold-match:")
    assert pod.image_pull_policy == "Always"


def test_pod_declares_no_credentials_before_it_runs():
    """Airflow snapshots rendered template fields (and the KPO pod YAML) into
    the metadata DB BEFORE pre_execute; env resolved in pre_execute never
    reaches that snapshot."""
    assert _DAG.get_task("match_pod").env_vars == []


def test_rebuild_targets_the_scheduled_build_with_an_honest_cause():
    """A hardcoded 'rollback' cause once got a healthy rebuild cancelled by a
    teammate acting reasonably on what it said; the cause is an interface."""
    rebuild = _DAG.get_task("rebuild")
    assert rebuild.job_id == 70471823431462
    assert rebuild.trigger_reason.startswith("gold-match daily: post-write rebuild")
    assert "{{ dag_run.start_date }}" in rebuild.trigger_reason
    assert rebuild.wait_for_termination is True


def test_cause_strings_are_the_agreed_literals():
    """The cause string is an operator interface; the rollback wording
    survives only on the supervised rollback path, which exact equality
    already enforces."""
    module = _dag_module()
    assert module.POST_WRITE_CAUSE_PREFIX == "gold-match daily: post-write rebuild"
    assert module.CLEANUP_CAUSE_PREFIX == "gold-match daily: cleanup rebuild after failed run"


def _dag_module():
    """The (hash-prefixed, DagBag-assigned) module the DAG file was imported
    under, recovered via a task's python_callable rather than a fixed name."""
    return sys.modules[_DAG.get_task("gates").python_callable.__module__]


_FAKE_DAG_RUN = SimpleNamespace(start_date=datetime(2026, 9, 2, 14, 30, 3, 123456, tzinfo=UTC))


@contextmanager
def _pod_runtime(module, *, pull_secret="", env=None):
    """Patch what _GoldMatchPodOperator.pre_execute reaches for at task runtime."""
    with (
        patch.object(module, "Variable", autospec=True) as mock_variable,
        patch.object(
            module,
            "gold_match_pod_env",
            autospec=True,
            return_value=env if env is not None else {"DATABRICKS_SERVER_HOSTNAME": "dbc.example"},
        ),
    ):
        mock_variable.get.return_value = pull_secret
        yield


def test_pull_secret_set_attaches_exactly_one_reference():
    """The package is private (plan of record), so the pod pulls with the
    Astro-provisioned secret named by the Variable."""
    module = _dag_module()
    op = module._match_pod()
    with _pod_runtime(module, pull_secret="gold-match-ghcr-pull"):
        op.pre_execute({})
    assert len(op.image_pull_secrets) == 1
    assert op.image_pull_secrets[0].name == "gold-match-ghcr-pull"


def test_pre_execute_loads_the_pods_env_from_the_shared_helper():
    """The pod env comes from gold_match_pod_env — the one place that maps the
    Airflow connection onto the CLIENT'S env names — so the pod and the gate
    tasks cannot drift on what they require."""
    module = _dag_module()
    op = module._match_pod()
    env = {"DATABRICKS_SERVER_HOSTNAME": "dbc.example", "BRAINTRUST_API_KEY": "bk"}
    with _pod_runtime(module, env=env):
        op.pre_execute({})
    assert {var.name: var.value for var in op.env_vars} == env


def _gates_with_metrics(module, metrics):
    gates_fn = _DAG.get_task("gates").python_callable
    with (
        patch.object(module, "connect_from_conn_id", autospec=True, return_value=MagicMock()),
        patch.object(module, "run_gate_queries", autospec=True, return_value=metrics),
    ):
        return gates_fn(dag_run=_FAKE_DAG_RUN)


def test_gates_fail_only_when_destroying_this_run_is_the_remedy():
    """Run-scoped dead tuples fail gates (cleanup deletes this run); a global
    count alone must NOT — deleting this run cannot clear an older run's
    tuple, so that observation rides operator_signal instead."""
    module = _dag_module()
    with pytest.raises(AirflowException, match="destroying this run"):
        _gates_with_metrics(module, {"run_scoped_dead": 2, "global_dead": 2})
    assert _gates_with_metrics(module, {"run_scoped_dead": 0, "global_dead": 3}) == 3


def test_signal_raises_for_first_quarantines_and_older_dead_labels_only():
    """Each failure story a human must see, without deleting anything: a first
    quarantine entry, or a global label warn with the run-scoped count zero."""
    module = _dag_module()
    signal_fn = _DAG.get_task("operator_signal").python_callable

    def run(fresh, gates_xcom):
        ti = MagicMock()
        ti.xcom_pull.return_value = gates_xcom
        with (
            patch.object(module, "connect_from_conn_id", autospec=True, return_value=MagicMock()),
            patch.object(module, "new_quarantine_count", autospec=True, return_value=fresh),
        ):
            signal_fn(dag_run=_FAKE_DAG_RUN, ti=ti)

    with pytest.raises(AirflowException, match="quarantine"):
        run(fresh=1, gates_xcom=0)
    with pytest.raises(AirflowException, match="OLDER"):
        run(fresh=0, gates_xcom=4)
    run(fresh=0, gates_xcom=0)  # nothing to signal
    run(fresh=0, gates_xcom=None)  # gates never ran (upstream failed); quarantine-only read


def test_cleanup_cancels_deletes_always_rebuilds_and_reraises():
    """The finalizer's contract: confirm any live rebuild is terminal BEFORE
    touching rows, delete by key, ALWAYS repair-rebuild, then re-raise so the
    DAG run ends FAILED and the existing alert fires."""
    module = _dag_module()
    cleanup_fn = _DAG.get_task("cleanup_finalizer").python_callable
    order = []
    ti = MagicMock()
    ti.xcom_pull.return_value = 555  # rebuild's early-pushed job_run_id
    with (
        patch.object(module, "DbtCloudHook", autospec=True),
        patch.object(module, "connect_from_conn_id", autospec=True, return_value=MagicMock()),
        patch.object(
            module,
            "cancel_dbt_run_and_confirm",
            autospec=True,
            side_effect=lambda *a, **k: order.append("cancel"),
        ) as mock_cancel,
        patch.object(
            module,
            "delete_run_rows",
            autospec=True,
            side_effect=lambda *a, **k: order.append("delete") or 7,
        ),
        patch.object(
            module,
            "trigger_rebuild_and_wait",
            autospec=True,
            side_effect=lambda *a, **k: order.append("rebuild") or 1,
        ) as mock_trigger,
        pytest.raises(AirflowException, match="cleanup completed"),
    ):
        cleanup_fn(dag_run=_FAKE_DAG_RUN, ti=ti)
    assert order == ["cancel", "delete", "rebuild"]
    # The resolved run id must reach the cancel call: pulling the XCom
    # correctly but cancelling a different id would leave a live rebuild
    # running while cleanup deletes and rebuilds against it.
    assert mock_cancel.call_args.args[1] == 555
    assert mock_trigger.call_args.kwargs["cause"].startswith(module.CLEANUP_CAUSE_PREFIX)
    ti.xcom_pull.assert_called_once_with(task_ids="rebuild", key="job_run_id")


def test_cleanup_skips_cancel_when_no_rebuild_run_was_ever_triggered():
    """job_run_id is pushed BEFORE the operator waits, so its absence means the
    trigger itself never fired — nothing to cancel, but delete + repair
    rebuild must still run."""
    module = _dag_module()
    cleanup_fn = _DAG.get_task("cleanup_finalizer").python_callable
    ti = MagicMock()
    ti.xcom_pull.return_value = None
    with (
        patch.object(module, "DbtCloudHook", autospec=True),
        patch.object(module, "connect_from_conn_id", autospec=True, return_value=MagicMock()),
        patch.object(module, "cancel_dbt_run_and_confirm", autospec=True) as mock_cancel,
        patch.object(module, "delete_run_rows", autospec=True, return_value=0) as mock_delete,
        patch.object(module, "trigger_rebuild_and_wait", autospec=True) as mock_trigger,
        pytest.raises(AirflowException, match="cleanup completed"),
    ):
        cleanup_fn(dag_run=_FAKE_DAG_RUN, ti=ti)
    assert not mock_cancel.called
    assert mock_delete.called
    assert mock_trigger.called


def test_cleanup_rebuilds_even_when_the_delete_raises():
    """A DELETE can commit and then raise on its response or teardown; the
    repair rebuild must still run (else serving keeps ghost rows the source
    lost), and the final exception must surface the delete failure."""
    module = _dag_module()
    cleanup_fn = _DAG.get_task("cleanup_finalizer").python_callable
    ti = MagicMock()
    ti.xcom_pull.return_value = None
    with (
        patch.object(module, "DbtCloudHook", autospec=True),
        patch.object(module, "connect_from_conn_id", autospec=True, return_value=MagicMock()),
        patch.object(module, "delete_run_rows", autospec=True, side_effect=RuntimeError("post-commit")),
        patch.object(module, "trigger_rebuild_and_wait", autospec=True) as mock_trigger,
        pytest.raises(AirflowException, match="cleanup delete raised"),
    ):
        cleanup_fn(dag_run=_FAKE_DAG_RUN, ti=ti)
    assert mock_trigger.called
