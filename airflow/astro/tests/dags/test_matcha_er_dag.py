"""Structure assertions for the matcha entity-resolution DAG.

Loaded from the file path directly rather than the configured dags_folder,
matching test_dag_example.py: CI does not point dags_folder at astro/dags, and
building the DagBag at collection time keeps this on real Airflow with no
metastore dependency.
"""

import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.models import DagBag
from include.custom_functions.matcha_utils import ENTITIES as _ENTITY_SPECS


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "matcha_er.py")
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("matcha_er")

_ENTITIES = ("candidacy_stage", "elected_official", "election_stage")


def test_dag_loads():
    assert _DAG is not None, f"matcha_er failed to load from {_DAG_FILE}"


def test_weekly_and_paused_on_creation():
    """Created paused so a fresh deploy does not auto-fire the current interval
    — catchup=False only suppresses historical backfill, not the current one."""
    # Airflow 3 expands schedule="@weekly" to a CronTriggerTimetable at parse
    # time rather than keeping the preset string; "0 0 * * 0" is its cron form.
    assert _DAG.timetable.expression == "0 0 * * 0"
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.catchup is False


def test_retries_meet_the_repo_floor():
    """test_dag_example.py::test_dag_retries asserts >= 2 for every DAG."""
    assert _DAG.default_args["retries"] >= 2


def test_every_entity_has_the_three_step_chain():
    for entity in _ENTITIES:
        match = _DAG.get_task(f"{entity}.match")
        gate = _DAG.get_task(f"{entity}.gate")
        swap = _DAG.get_task(f"{entity}.swap")
        assert "match" in {t.task_id.split(".")[-1] for t in gate.upstream_list}
        assert "gate" in {t.task_id.split(".")[-1] for t in swap.upstream_list}
        assert match is not None


def test_match_tasks_share_the_serialising_pool():
    """Three 8Gi pods in parallel exceed the 20Gi deployment quota, so the pool
    holds them to one at a time until the quota is raised."""
    for entity in _ENTITIES:
        assert _DAG.get_task(f"{entity}.match").pool == "matcha_er"


def test_entities_are_independent_of_each_other():
    """Within this DAG, each entity's match depends only on
    dbt_refresh_prematch — one entity failing must not block another from
    matching, gating, or swapping. This independence is scheduling-only: a
    civics mart reading er_source downstream can still see one entity's
    fresh vintage next to another's stale one, since nothing here gates a
    mart on all three swaps."""
    for entity in _ENTITIES:
        upstream = {t.task_id for t in _DAG.get_task(f"{entity}.match").upstream_list}
        assert upstream == {"dbt_refresh_prematch"}


def test_downstream_dbt_waits_for_every_swap():
    """Gates only THIS DAG's own staging rebuild on all three swaps — it does
    not prevent er_source itself from holding a mixed vintage. If one
    entity's swap fails after the other two already succeeded, those two are
    already live; only this DAG's dbt_build_er_source is withheld until a
    retry clears the failure."""
    upstream = {t.task_id for t in _DAG.get_task("dbt_build_er_source").upstream_list}
    assert upstream == {f"{e}.swap" for e in _ENTITIES}


def test_cleanup_runs_after_the_downstream_build():
    """The _old tables are the rollback position until dbt has read the new one."""
    upstream = {t.task_id for t in _DAG.get_task("cleanup").upstream_list}
    assert upstream == {"dbt_build_er_source"}
    assert _DAG.get_task("cleanup").downstream_list == []


def test_pods_write_dated_tables_never_live():
    """matcha's upload is CREATE OR REPLACE + COPY INTO, so aiming it at a live
    table would let a mid-upload failure empty what dbt reads."""
    for entity in _ENTITIES:
        args = " ".join(_DAG.get_task(f"{entity}.match").arguments)
        assert "ds_nodash" in args
        assert "--overwrite" in args


def test_match_pod_targets_its_own_entity():
    """`_match_pod(entity)` is called synchronously inside `group()`, so its
    `arguments` list is fully resolved at DAG-build time and was never
    vulnerable to a late-binding bug — this only guards against some other
    regression breaking the per-entity wiring.
    """
    for entity in _ENTITY_SPECS:
        args = _DAG.get_task(f"{entity.entity_type}.match").arguments
        assert args[args.index("--entity-type") + 1] == entity.entity_type
        joined = " ".join(args)
        assert entity.cluster_table in joined
        assert entity.pairwise_table in joined


def _dag_module():
    """The (hash-prefixed, DagBag-assigned) module matcha_er.py was imported
    under, recovered via a task's python_callable rather than a fixed name."""
    return sys.modules[_DAG.get_task("candidacy_stage.gate").python_callable.__module__]


def test_pull_secret_unset_leaves_the_pod_pulling_anonymously():
    """image_pull_secrets holds Kubernetes client objects, not strings, so
    Jinja cannot template it — _MatchaPodOperator.pre_execute resolves it at
    task runtime instead. Confirmed (not assumed) that a freshly-built
    KubernetesPodOperator's own default is `[]`, not `None`, before asserting
    an unset/empty Variable leaves it there: the matcha image is still public
    today, so the pod must carry no imagePullSecrets at all.

    Uses a fresh operator from _match_pod rather than a task pulled off the
    shared _DAG (whose task objects are the same instance across every test
    in this module, so mutating one's image_pull_secrets would leak state).
    """
    module = _dag_module()
    op = module._match_pod(_ENTITY_SPECS[0])
    assert op.image_pull_secrets == []  # KubernetesPodOperator's real instantiated default
    with patch.object(module, "Variable", autospec=True) as mock_variable:
        mock_variable.get.return_value = ""
        op.pre_execute({})
    assert op.image_pull_secrets == []


def test_pull_secret_set_attaches_exactly_one_reference():
    module = _dag_module()
    op = module._match_pod(_ENTITY_SPECS[0])
    with patch.object(module, "Variable", autospec=True) as mock_variable:
        mock_variable.get.return_value = "matcha-ghcr-pull"
        op.pre_execute({})
    assert len(op.image_pull_secrets) == 1
    assert op.image_pull_secrets[0].name == "matcha-ghcr-pull"


def test_match_pods_set_the_pull_policy_explicitly():
    """Kubernetes derives an unset policy FROM THE TAG — Always for `:latest`,
    IfNotPresent otherwise — so pinning `matcha_image_tag` to a sha would also
    flip the pull behavior as a side effect. Always is the deliberate choice
    over IfNotPresent: a node-local cache can hold a matcher build older than
    the tag now points at and would run it silently, and it buys next to no
    coherence between this run's pods, which the pool serializes onto
    generally separate nodes. Coherence comes from pinning the tag instead.
    """
    for entity in _ENTITIES:
        assert _DAG.get_task(f"{entity}.match").image_pull_policy == "Always"


def test_a_mutable_tag_warns_that_the_run_is_not_reproducible():
    """A merge touching matcha/** republishes `latest`, and the pool runs the
    three pods one after another, so a run on a mutable tag can execute two
    different matcher builds. The run's own logs have to say so — otherwise a
    gate failure looks like a data problem.

    `image` is set on the operator directly here rather than left as its Jinja
    template: pre_execute runs after the task instance renders template
    fields, so a real run sees the resolved string.
    """
    module = _dag_module()
    op = module._match_pod(_ENTITY_SPECS[0])
    op.image = "ghcr.io/thegoodparty/gp-data-platform/matcha:latest"
    with (
        patch.object(module, "Variable", autospec=True) as mock_variable,
        patch.object(module, "t_log", autospec=True) as mock_log,
    ):
        mock_variable.get.return_value = ""
        op.pre_execute({})
    assert mock_log.warning.called
    assert not mock_log.info.called
    assert "latest" in mock_log.warning.call_args.args[1]


def test_a_sha_pinned_tag_logs_the_image_without_warning():
    module = _dag_module()
    for image in (
        f"ghcr.io/thegoodparty/gp-data-platform/matcha:{'a1b2c3d4' * 5}",
        f"ghcr.io/thegoodparty/gp-data-platform/matcha@sha256:{'ab' * 32}",
    ):
        op = module._match_pod(_ENTITY_SPECS[0])
        op.image = image
        with (
            patch.object(module, "Variable", autospec=True) as mock_variable,
            patch.object(module, "t_log", autospec=True) as mock_log,
        ):
            mock_variable.get.return_value = ""
            op.pre_execute({})
        assert not mock_log.warning.called, image
        assert mock_log.info.call_args.args[1] == image


def test_gate_task_checks_its_own_entitys_tables():
    """`gate` and `swap` read `entity` from the enclosing closure at task
    EXECUTION time, unlike `match`'s eagerly-resolved arguments — so a bare
    `for entity in ENTITIES:` loop in place of the `entity_group` factory
    would pass every structural test above while every group's gate/swap
    silently operated on the same (last-iteration) entity's tables. DagBag
    exposes the TaskFlow-wrapped closure via `python_callable`, so invoke it
    directly and assert it gates THAT group's own tables.

    Checks table (args[3]) paired with its OWN gate (args[5]), not just that
    the right tables and gates each showed up somewhere: a swapped pairing
    (cluster table checked against the pairwise gate, or vice versa) would
    under-gate the cluster table — silently dropping its identity/source
    checks — which is exactly the failure class this branch exists to catch.
    Index 4 (the dated table name) is skipped: it's derived from args[3] in
    the same expression and cannot diverge from it independently.
    """
    # autospec=True on every patch below: a plain MagicMock accepts any keyword argument
    # silently, which is exactly how a wrong kwarg on a real call (e.g. Variable.get's
    # Airflow-2 `default_var=` instead of `default=`) once passed every test here while
    # raising TypeError at runtime. autospec enforces the real signatures so a call using a
    # keyword/arity the function doesn't have fails the test instead of hiding.
    module = _dag_module()
    for entity in _ENTITY_SPECS:
        gate_fn = _DAG.get_task(f"{entity.entity_type}.gate").python_callable
        with (
            patch.object(module, "open_connection", autospec=True, return_value=MagicMock()),
            patch.object(module, "Variable", autospec=True) as mock_variable,
            patch.object(module, "run_gate", autospec=True) as mock_run_gate,
        ):
            mock_variable.get.return_value = "cat"
            gate_fn("20260825")
        pairs = {(call.args[3], call.args[5]) for call in mock_run_gate.call_args_list}
        assert pairs == {
            (entity.cluster_table, entity.cluster_gate),
            (entity.pairwise_table, entity.pairwise_gate),
        }


def test_swap_task_swaps_its_own_entitys_tables():
    """Same closure risk as `gate`, exercised with the rename path armed.

    `swap_table`'s call carries no gate-like argument to pair (conn, catalog,
    schema, table, dated_table only), so there is no equivalent args[5] to
    tighten this against — the table-name check is already the full claim.
    """
    module = _dag_module()
    for entity in _ENTITY_SPECS:
        swap_fn = _DAG.get_task(f"{entity.entity_type}.swap").python_callable
        with (
            patch.object(module, "swap_enabled", autospec=True, return_value=True),
            patch.object(module, "open_connection", autospec=True, return_value=MagicMock()),
            patch.object(module, "Variable", autospec=True) as mock_variable,
            patch.object(module, "swap_table", autospec=True) as mock_swap_table,
        ):
            mock_variable.get.return_value = "cat"
            swap_fn("20260825")
        tables = {call.args[3] for call in mock_swap_table.call_args_list}
        assert tables == {entity.cluster_table, entity.pairwise_table}
