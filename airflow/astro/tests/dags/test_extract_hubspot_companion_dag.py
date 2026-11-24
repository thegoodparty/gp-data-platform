"""Structure and wiring of the HubSpot companion activity-association loader DAG."""

import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

DagBag = pytest.importorskip("airflow.models").DagBag
NullTimetable = pytest.importorskip("airflow.sdk.definitions.timetables.simple").NullTimetable


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


_DAG_FILE = str(Path(__file__).resolve().parents[2] / "dags" / "extract_hubspot_companion.py")
with suppress_logging("airflow"):
    _DAG = DagBag(dag_folder=_DAG_FILE).dags.get("extract_hubspot_companion")

VARIABLES = {
    "hubspot_companion_loader_token": "pat-test-token",
    "hubspot_companion_landing_schema": "dev.hubspot",
    "hubspot_companion_activity_source": "goodparty_data_catalog.lakeflow_source_dev",
}


def _globals(task_id="extract_calls"):
    """The task callable's own globals, the dict it resolves names from.

    The DagBag imports the DAG file itself, so patching a module the test imported would
    hit a different instance.
    """
    operator = _DAG.get_task(task_id)
    callable_ = getattr(operator, "python_callable", None)
    assert callable_ is not None, f"{task_id} has no python callable"
    return callable_.__globals__


def test_dag_loads():
    assert _DAG is not None, f"extract_hubspot_companion failed to load from {_DAG_FILE}"


def test_the_dag_deploys_paused_one_run_at_a_time_and_carries_no_schedule():
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.max_active_runs == 1
    assert _DAG.catchup is False
    # The cadence is the owner's call, so a schedule appearing here is a decision, not a slip.
    assert isinstance(_DAG.timetable, NullTimetable)


def test_one_task_per_activity_type():
    assert {task.task_id for task in _DAG.tasks} == {
        "extract_calls",
        "extract_meetings",
        "extract_notes",
        "extract_tasks",
    }


def test_the_tasks_are_independent_so_one_slow_type_cannot_hold_the_rest():
    for task in _DAG.tasks:
        assert task.upstream_list == []


def test_the_defaults_point_at_the_dev_landing_and_the_lakeflow_source():
    """Both are Variables so the D3 cutover is configuration, not a code change."""
    params = {name: param.value for name, param in _DAG.params.items()}
    assert params == {"activities": [], "full_sweep": False}
    globals_ = _globals()
    assert globals_["LANDING_SCHEMA_VARIABLE"] == "hubspot_companion_landing_schema"
    assert globals_["ACTIVITY_SOURCE_VARIABLE"] == "hubspot_companion_activity_source"
    assert globals_["TOKEN_VARIABLE"] == "hubspot_companion_loader_token"
    assert globals_["SCHEDULE"] is None


def test_the_activity_names_are_the_four_types_with_an_association_endpoint():
    assert _globals()["_ACTIVITY_NAMES"] == ("calls", "meetings", "notes", "tasks")


@pytest.mark.parametrize(
    ("requested", "activity_object", "expected"),
    [
        ([], "calls", True),
        (["calls"], "calls", True),
        (["calls"], "notes", False),
        (["meetings", "notes"], "notes", True),
    ],
)
def test_the_activities_param_narrows_a_run(requested, activity_object, expected):
    should_extract = _globals()["should_extract"]
    assert should_extract(activity_object, requested) is expected


def test_an_unknown_activity_name_raises_rather_than_reading_nothing():
    """A typo in a manual trigger would otherwise be a green run that read nothing."""
    with pytest.raises(ValueError, match="Unknown activities"):
        _globals()["should_extract"]("calls", ["call"])


def test_a_landing_schema_variable_splits_into_catalog_and_schema():
    assert _globals()["_split_schema"]("dev.hubspot", "x") == ("dev", "hubspot")


@pytest.mark.parametrize("value", ["", "hubspot", "a.b.c", "a."])
def test_a_malformed_landing_schema_variable_names_itself(value):
    with pytest.raises(ValueError, match="x must be"):
        _globals()["_split_schema"](value, "x")


class FakeConnection:
    closed = False

    def close(self):
        self.closed = True


def _run_extract(task_id="extract_calls", params=None, variables=None):
    """Run one task's callable with the Variables and the Databricks connection stubbed."""
    variables = VARIABLES if variables is None else variables
    captured = {}

    def get(name, default=None):
        if name in variables:
            return variables[name]
        if default is not None:
            return default
        raise KeyError(name)

    def fake_extract(connection, config, activity_object, activity_type, session=None):
        captured.update(
            config=config,
            activity_object=activity_object,
            activity_type=activity_type,
        )
        return {"activity_type": activity_type}

    connection = FakeConnection()
    globals_ = _globals(task_id)
    context = {
        "params": params or {"activities": [], "full_sweep": False},
        "dag_run": type("Run", (), {"run_id": "scheduled__run"})(),
    }
    with patch.dict(
        globals_,
        {
            "Variable": MagicMock(get=Mock(side_effect=get)),
            "connect_from_conn_id": MagicMock(return_value=connection),
            "extract_activity_associations": MagicMock(side_effect=fake_extract),
            "get_current_context": Mock(return_value=context),
        },
    ):
        result = _DAG.get_task(task_id).python_callable()
    return result, captured, connection


def test_a_task_builds_its_config_from_the_variables_at_run_time():
    result, captured, connection = _run_extract()

    assert result == {"activity_type": "CALL"}
    assert captured["activity_object"] == "calls"
    assert captured["activity_type"] == "CALL"
    config = captured["config"]
    assert config.catalog == "dev"
    assert config.schema == "hubspot"
    assert config.source_prefix == "goodparty_data_catalog.lakeflow_source_dev"
    assert config.api_token == "pat-test-token"
    assert config.dag_run_id == "scheduled__run"
    assert config.full_sweep is False
    assert connection.closed is True


def test_a_skipped_activity_opens_no_connection():
    result, captured, _ = _run_extract(
        task_id="extract_notes", params={"activities": ["calls"], "full_sweep": False}
    )

    assert result["skipped"] is True
    assert captured == {}


def test_the_token_is_stripped_so_a_pasted_newline_cannot_reach_the_header():
    _, captured, _ = _run_extract(
        variables={**VARIABLES, "hubspot_companion_loader_token": "  pat-test-token\n"}
    )

    assert captured["config"].api_token == "pat-test-token"


def test_a_full_sweep_param_reaches_the_config():
    _, captured, _ = _run_extract(params={"activities": [], "full_sweep": True})

    assert captured["config"].full_sweep is True
