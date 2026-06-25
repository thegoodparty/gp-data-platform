"""The loader DAG parses and exposes the expected steps in order."""

from __future__ import annotations

from pathlib import Path

from airflow.models import DagBag

_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "load_people_api_v2.py")


def _dag():
    bag = DagBag(dag_folder=_DAG_FILE, include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("load_people_api_v2")
    assert dag is not None
    return dag


def test_dag_imports_clean():
    _dag()


def test_step_sequence():
    dag = _dag()
    ids = set(dag.task_ids)
    expected = {
        "inspect_prod",
        "dbt_test_voter_gate",
        "unload",
        "provision",
        "create_schema",
        "copy",
        "build_indexes",
        "resize",
        "validate",
    }
    assert expected <= ids
    # gate blocks unload; provision runs parallel to unload after the gate
    assert "dbt_test_voter_gate" in {t.task_id for t in dag.get_task("unload").upstream_list}
    assert dag.get_task("resize").downstream_list[0].task_id == "validate"
