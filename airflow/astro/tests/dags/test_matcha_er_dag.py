"""Structure assertions for the matcha entity-resolution DAG.

Loaded from the file path directly rather than the configured dags_folder,
matching test_dag_example.py: CI does not point dags_folder at astro/dags, and
building the DagBag at collection time keeps this on real Airflow with no
metastore dependency.
"""

import logging
from contextlib import contextmanager
from pathlib import Path

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
    """Every assertion above is entity-agnostic (task ids, upstream sets, pool
    name, the ds_nodash/--overwrite substrings), so a factory bug that closes
    over the loop variable instead of taking `entity` as a parameter would
    still pass all of them while every group's pod actually matched
    `election_stage` (the last iteration value). This checks each group's pod
    is wired to ITS OWN entity's --entity-type flag and table names, which
    such a bug would break.

    The gate/swap tasks carry the identical closure risk but are TaskFlow
    task instances wrapping a Python closure; the DAG structure exposes no
    way to introspect which entity a closure captured, so that half of a
    late-binding regression is not structurally testable from here.
    """
    for entity in _ENTITY_SPECS:
        args = _DAG.get_task(f"{entity.entity_type}.match").arguments
        assert args[args.index("--entity-type") + 1] == entity.entity_type
        joined = " ".join(args)
        assert entity.cluster_table in joined
        assert entity.pairwise_table in joined
