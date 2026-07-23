"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors. This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import logging
import os
from contextlib import contextmanager
from pathlib import Path

import pytest
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


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag()

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [(strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag()

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# Built once at collection time (real Airflow), before sibling tests stub `airflow` in
# sys.modules — so reading DAG structure here is safe. Reused across the tests below to avoid
# rebuilding the DagBag per parametrize/ids call.
_IMPORT_ERRORS = get_import_errors()
_ALL_DAGS = get_dags()

# Load the loader DAG from its file directly — CI does not point the configured dags_folder at
# astro/dags (so get_dags() is empty there), but the file path is stable. Collection-time build
# means real Airflow (before the sibling airflow stub) and no metastore dependency.
_LOADER_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "load_people_api.py")
with suppress_logging("airflow"):
    # .dags is the in-memory parse result; .get_dag() would query the metastore (no DB in CI).
    _LOADER_DAG = DagBag(dag_folder=_LOADER_DAG_FILE).dags.get("load_people_api")


@pytest.mark.parametrize("rel_path,rv", _IMPORT_ERRORS, ids=[x[0] for x in _IMPORT_ERRORS])
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS: dict[str, str] = {}


@pytest.mark.parametrize("dag_id,dag,fileloc", _ALL_DAGS, ids=[x[2] for x in _ALL_DAGS])
def test_dag_tags(dag_id, dag, fileloc):
    """
    Test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize("dag_id,dag, fileloc", _ALL_DAGS, ids=[x[2] for x in _ALL_DAGS])
def test_dag_retries(dag_id, dag, fileloc):
    """
    Test if a DAG has retries set
    """
    assert dag.default_args.get("retries", None) >= 2, f"{dag_id} in {fileloc} must have task retries >= 2."


def test_load_people_api_sequence():
    """The loader DAG gates unload/provision on the dbt test and ends build_indexes -> validate ->
    resize: validate runs BEFORE resize so its heavy per-state counts run on the big index instance,
    not the small post-resize serving box.
    """
    assert _LOADER_DAG is not None, f"load_people_api failed to load from {_LOADER_DAG_FILE}"
    assert "dbt_test_voter_gate" in {t.task_id for t in _LOADER_DAG.get_task("unload").upstream_list}
    assert "dbt_test_voter_gate" in {t.task_id for t in _LOADER_DAG.get_task("provision").upstream_list}
    assert "build_indexes" in {t.task_id for t in _LOADER_DAG.get_task("validate").upstream_list}
    assert "validate" in {t.task_id for t in _LOADER_DAG.get_task("resize").upstream_list}
    # resize must not be upstream of validate anymore (the old order is fully gone).
    assert "resize" not in {t.task_id for t in _LOADER_DAG.get_task("validate").upstream_list}


def test_load_people_api_scale_down_on_failure():
    """scale_down_on_failure is the on-failure cost guard: downstream of every task after which a
    cluster can exist (provision, unload, the serial load chain, AND validate — which now runs on
    the scaled-up writer before resize, so a validate failure must flip it to serverless too),
    firing via trigger_rule=one_failed if any of them fails, but NOT upstream of resize (a
    successful run skips it since resize already made the writer serverless). `unload` must be a
    direct upstream: one_failed fires only on a FAILED direct upstream, and an unload failure with
    provision success leaves the rest UPSTREAM_FAILED, which does not satisfy one_failed.
    """
    assert _LOADER_DAG is not None, f"load_people_api failed to load from {_LOADER_DAG_FILE}"
    scale_down_task = _LOADER_DAG.get_task("scale_down_on_failure")
    assert scale_down_task.trigger_rule == "one_failed"
    upstream_ids = {t.task_id for t in scale_down_task.upstream_list}
    assert upstream_ids == {
        "provision",
        "unload",
        "create_schema",
        "copy",
        "build_indexes",
        "validate",
        "resize",
    }
    assert "scale_down_on_failure" not in {t.task_id for t in _LOADER_DAG.get_task("resize").upstream_list}
