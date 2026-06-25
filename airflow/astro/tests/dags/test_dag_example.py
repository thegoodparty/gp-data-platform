"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors. This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import logging
import os
from contextlib import contextmanager

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
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [(strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# Built once at collection time (real Airflow), before sibling tests stub `airflow` in
# sys.modules — so reading DAG structure here is safe. Reused across the tests below to avoid
# rebuilding the DagBag per parametrize/ids call.
_IMPORT_ERRORS = get_import_errors()
_ALL_DAGS = get_dags()
_DAGS_BY_ID = {d[0]: d[1] for d in _ALL_DAGS}


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
    """The loader DAG gates unload/provision on the dbt test and ends resize -> validate."""
    dag = _DAGS_BY_ID.get("load_people_api")
    assert dag is not None, "load_people_api DAG not found in the DagBag"
    assert "dbt_test_voter_gate" in {t.task_id for t in dag.get_task("unload").upstream_list}
    assert "dbt_test_voter_gate" in {t.task_id for t in dag.get_task("provision").upstream_list}
    assert "resize" in {t.task_id for t in dag.get_task("validate").upstream_list}
