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
    """No dbt model joins two entities' cluster tables, so one entity failing
    must not block the others matching and swapping."""
    for entity in _ENTITIES:
        upstream = {t.task_id for t in _DAG.get_task(f"{entity}.match").upstream_list}
        assert upstream == {"dbt_refresh_prematch"}


def test_downstream_dbt_waits_for_every_swap():
    """Withhold the downstream build rather than publish a mixed vintage."""
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
    module = _dag_module()
    for entity in _ENTITY_SPECS:
        gate_fn = _DAG.get_task(f"{entity.entity_type}.gate").python_callable
        with (
            patch.object(module, "open_connection", return_value=MagicMock()),
            patch.object(module, "Variable") as mock_variable,
            patch.object(module, "run_gate") as mock_run_gate,
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
            patch.object(module, "swap_enabled", return_value=True),
            patch.object(module, "open_connection", return_value=MagicMock()),
            patch.object(module, "Variable") as mock_variable,
            patch.object(module, "swap_table") as mock_swap_table,
        ):
            mock_variable.get.return_value = "cat"
            swap_fn("20260825")
        tables = {call.args[3] for call in mock_swap_table.call_args_list}
        assert tables == {entity.cluster_table, entity.pairwise_table}
