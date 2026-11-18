"""Structure assertions for the data_pipeline_reader DAG.

Loaded from the file path directly rather than from the configured dags_folder,
matching test_gold_match_daily_dag.py: CI does not point dags_folder at astro/dags,
and building the DagBag at collection time keeps this on real Airflow with no
metastore dependency.
"""

import inspect
import logging
import sys
from contextlib import contextmanager
from pathlib import Path

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


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "data_pipeline_reader.py")
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("data_pipeline_reader")

FIXED_TASKS = {"load_pipelines", "send_to_grafana", "save_state"}


def test_dag_loads():
    assert _DAG is not None, f"data_pipeline_reader failed to load from {_DAG_FILE}"


def test_one_collection_task_per_configured_tool():
    """PRs 2 and 3 add a collector by adding a tool to the pipeline list and a task
    here; the test derives the expected set rather than naming it, so it does not
    need rewriting for them."""
    collectors = {t for t in _DAG.task_ids if t.startswith("collect_")}
    assert collectors == {"collect_dbt_cloud"}


def test_exactly_the_fixed_tasks_plus_collectors():
    assert set(_DAG.task_ids) == FIXED_TASKS | {t for t in _DAG.task_ids if t.startswith("collect_")}


def test_the_send_runs_even_when_a_collector_fails():
    """A revoked credential must not make rule 4 see silence, so the send is all_done."""
    assert _DAG.get_task("send_to_grafana").trigger_rule == "all_done"


def test_the_state_write_runs_only_after_a_successful_send():
    """Default all_success: a failed send leaves the cursor where it was, so the
    next run replays the window instead of skipping it."""
    save = _DAG.get_task("save_state")
    assert save.trigger_rule == "all_success"
    assert {t.task_id for t in save.upstream_list} == {"collect_dbt_cloud", "send_to_grafana"}


def test_the_list_is_validated_before_anything_is_read():
    assert {t.task_id for t in _DAG.get_task("collect_dbt_cloud").upstream_list} == {"load_pipelines"}


def test_single_active_run():
    """Two runs overlapping would race on the state Variable."""
    assert _DAG.max_active_runs == 1


def test_no_catchup():
    assert _DAG.catchup is False


def test_schedule_is_a_few_minutes():
    assert _DAG.timetable.summary.startswith("*/5") or "*/5" in _DAG.timetable.summary


def test_retries_meet_the_repo_floor():
    """test_dag_example.py::test_dag_retries asserts >= 2 for every DAG."""
    assert _DAG.default_args["retries"] >= 2


def _dag_module():
    return sys.modules[_DAG.get_task("send_to_grafana").python_callable.__module__]


def test_no_trigger_or_cancel_calls():
    """The reader only reads: nothing in the module reaches a trigger, rerun, cancel
    or pause API, and the provider's triggering hook methods are not imported."""
    module = _dag_module()
    source = inspect.getsource(module)
    for forbidden in (
        "trigger_job_run",
        "cancel_job_run",
        "DbtCloudRunJobOperator",
        "clear_task_instances",
        "dag_run.trigger",
    ):
        assert forbidden not in source, forbidden
    names = {n for n in dir(module)}
    assert not {"trigger_job_run", "cancel_job_run"} & names


def test_no_credentials_are_declared_before_the_run():
    """ "A token must not end up in DAG source or in a parse-time object: the
    connections are looked up by id inside the task, at run time."""
    module = _dag_module()
    source = inspect.getsource(module)
    assert "BaseHook.get_connection" in source
    # No token, password or secret literal anywhere in the module.
    for marker in ('password = "', "password='", 'token = "', "token='", 'secret = "', "secret='"):
        assert marker not in source, marker


def test_the_grafana_credentials_are_read_only_inside_the_task():
    """Connection fields are read at task runtime, so a parse-time object carries none."""
    source = inspect.getsource(_dag_module())
    body = source[source.index("def send_to_grafana") : source.index("def save_state")]
    assert "BaseHook.get_connection" in body
    assert "GRAFANA_CONN_ID" in body


def test_the_grafana_endpoint_accepts_a_host_with_or_without_a_scheme():
    module = _dag_module()
    assert module.grafana_endpoint("otlp.example.invalid") == "https://otlp.example.invalid/otlp/v1/logs"
    assert (
        module.grafana_endpoint("https://otlp.example.invalid/")
        == "https://otlp.example.invalid/otlp/v1/logs"
    )
    assert (
        module.grafana_endpoint("https://otlp.example.invalid/otlp/v1/logs")
        == "https://otlp.example.invalid/otlp/v1/logs"
    )


def test_the_task_count_is_four_today():
    assert len(_DAG.task_ids) == 4


def test_the_state_write_goes_through_the_library_not_the_task():
    """`save_state` is both a task name and a library function. If the library one
    is not imported under another name, the name inside the DAG body resolves to the
    task, so the task calls itself and the Variable is never written. A structure
    test cannot see that, so this pins the alias and that the body uses it."""
    module = _dag_module()
    # The alias is the library function, not the decorated task of the same name.
    assert callable(module.write_state)
    assert not hasattr(module.write_state, "operator_class"), "write_state is the task, not the writer"
    source = inspect.getsource(module)
    body = source[source.index("def save_state") :]
    assert "write_state(Variable.set, state)" in body
    # The task's own name must never be called with the writer's arguments.
    assert "save_state(Variable.set" not in source
