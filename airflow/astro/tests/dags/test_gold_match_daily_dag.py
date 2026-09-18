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
    """14:30 UTC sits after the 08:00 L2 load and the 12:02 build; the rows
    the pod writes are published by the NEXT scheduled build, so the cron only
    needs to follow the universe movers. Paused on creation because BUILD must
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


def test_exactly_three_tasks_and_no_dbt_trigger():
    """The loop is match-then-let-the-nightly-publish: a rebuild, gate or
    cleanup task creeping back would re-couple publication to a build the
    DAG owns and re-open the rollback-for-unrelated-reasons class."""
    assert set(_DAG.task_ids) == {"admission", "match_pod", "operator_signal"}
    for t in _DAG.tasks:
        assert "Dbt" not in type(t).__name__, type(t).__name__


def test_single_attempt_tasks():
    """admission: its checks are snapshots, so a retry minutes later could
    admit a day the first attempt declined. match_pod: the daily retry is
    tomorrow's run — a same-key retry reopens the resume/overlap states the
    design removed."""
    for task_id in ("admission", "match_pod"):
        assert _DAG.get_task(task_id).retries == 0, task_id


def test_pipeline_order_and_signal_runs_regardless():
    """admission gates the pod; operator_signal runs on all_done so a declined
    day (pod skipped) still signals its reason."""
    assert {t.task_id for t in _DAG.get_task("match_pod").upstream_list} == {"admission"}
    signal = _DAG.get_task("operator_signal")
    assert signal.trigger_rule == "all_done"
    assert {t.task_id for t in signal.upstream_list} == {"match_pod"}
    assert _DAG.get_task("admission").ignore_downstream_trigger_rules is False


def test_pod_runs_the_daily_module_with_the_dagrun_key():
    """The image entrypoint is `python -m`, so args[0] must be the module; the
    run key is the DagRun's own timestamp templated in — no minting task, no
    XCom, and the operator can always re-derive the same key for a delete."""
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


def _dag_module():
    """The (hash-prefixed, DagBag-assigned) module the DAG file was imported
    under, recovered via a task's python_callable rather than a fixed name."""
    return sys.modules[_DAG.get_task("operator_signal").python_callable.__module__]


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
    Airflow connection onto the CLIENT'S env names — so the pod and the signal
    task cannot drift on what they require."""
    module = _dag_module()
    op = module._match_pod()
    env = {"DATABRICKS_SERVER_HOSTNAME": "dbc.example", "BRAINTRUST_API_KEY": "bk"}
    with _pod_runtime(module, env=env):
        op.pre_execute({})
    assert {var.name: var.value for var in op.env_vars} == env


def _admit(module, *, latest=(True, "job 462 run 1 (SUCCESS)"), live=lambda hook: [], latest_raises=None):
    admission_fn = _DAG.get_task("admission").python_callable
    ti = MagicMock()
    with (
        patch.object(module, "DbtCloudHook", autospec=True),
        patch.object(
            module,
            "latest_scheduled_build_succeeded",
            autospec=True,
            side_effect=latest_raises if latest_raises else (lambda hook: latest),
        ),
        patch.object(module, "inflight_prod_builds", autospec=True, side_effect=live),
    ):
        admitted = admission_fn(ti=ti)
    pushed = ti.xcom_push.call_args.kwargs["value"] if ti.xcom_push.called else None
    return admitted, pushed


def test_admission_proceeds_when_the_nightly_succeeded_and_nothing_is_in_flight():
    module = _dag_module()
    admitted, pushed = _admit(module)
    assert admitted is True and pushed is None


def test_admission_declines_when_the_latest_scheduled_build_did_not_succeed():
    """A red nightly means yesterday's universe and marts, and if the matcher's
    own rows made it red the operator must remove them first; writing more
    on top would compound it."""
    module = _dag_module()
    admitted, pushed = _admit(module, latest=(False, "job 462 run 9 (ERROR)"))
    assert admitted is False
    assert "did not succeed" in pushed and "run 9 (ERROR)" in pushed


def test_admission_declines_while_a_prod_build_is_in_flight():
    """Two prod builds on the same tables lost a mart write on 2026-09-17."""
    module = _dag_module()
    admitted, pushed = _admit(module, live=lambda hook: ["job 70471823431463 run 9 (RUNNING)"])
    assert admitted is False
    assert "in flight" in pushed and "70471823431463" in pushed


def test_admission_fails_closed_when_dbt_cloud_cannot_be_asked():
    """If the checks cannot reach dbt Cloud the day is declined (nothing
    written), never written and hoped."""
    module = _dag_module()

    def boom(hook):
        raise ConnectionError("api down")

    admitted, pushed = _admit(module, latest_raises=boom)
    assert admitted is False
    assert "unreachable" in pushed


def _signal(module, *, fresh, declined=None):
    signal_fn = _DAG.get_task("operator_signal").python_callable
    ti = MagicMock()
    # Dispatch on (task_ids, key): a typo in the DAG's key would read None and
    # make a declined day look healthy, so the key is part of the contract.
    ti.xcom_pull.side_effect = lambda task_ids, key=None: {("admission", "declined_reason"): declined}.get(
        (task_ids, key)
    )
    with (
        patch.object(module, "connect_from_conn_id", autospec=True, return_value=MagicMock()),
        patch.object(module, "new_quarantine_count", autospec=True, return_value=fresh),
    ):
        signal_fn(dag_run=_FAKE_DAG_RUN, ti=ti)
    return ti


def test_signal_raises_for_first_quarantines_and_declined_days_only():
    """Each story a human must see, without deleting anything: offices that
    first entered quarantine this run (the operator's own adjudication holds
    are excluded in the helper), or a day declined at admission."""
    module = _dag_module()
    with pytest.raises(AirflowException, match="quarantine"):
        _signal(module, fresh=1)
    with pytest.raises(AirflowException, match="declined at admission.*in flight"):
        _signal(
            module, fresh=0, declined="another prod build is in flight: job 70471823431463 run 9 (RUNNING)"
        )
    ti = _signal(module, fresh=0)  # nothing to signal
    ti.xcom_pull.assert_any_call(task_ids="admission", key="declined_reason")
