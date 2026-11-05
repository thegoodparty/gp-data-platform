"""Structure and wiring of the HubSpot contact merge DAG."""

import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import DagBag

_DAG_FILE = Path(__file__).resolve().parents[2] / "dags" / "hubspot_contact_merge.py"
logging.getLogger("airflow").disabled = True
_DAG = DagBag(dag_folder=str(_DAG_FILE)).dags.get("hubspot_contact_merge")
logging.getLogger("airflow").disabled = False

_spec = importlib.util.spec_from_file_location("hubspot_contact_merge", _DAG_FILE)
dag_module = importlib.util.module_from_spec(_spec)
sys.modules["hubspot_contact_merge"] = dag_module
_spec.loader.exec_module(dag_module)

VARIABLES = {
    "hubspot_merge_hubspot_token": "pat-test",
    "hubspot_merge_expected_portal_id": "21589597",
    "hubspot_merge_pair_relation": "goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_queue",
    "hubspot_merge_log_table": "goodparty_data_catalog.airflow_source.hubspot_contact_merge_log",
    "hubspot_merge_max_pairs": "100",
}
CONN = {"host": "h", "http_path": "/p", "client_id": "id", "client_secret": "secret", "scopes": ["sql"]}


def _env(params, variables=VARIABLES):
    def get(name, default=None):
        if name in variables:
            return variables[name]
        if default is not None:
            return default
        raise KeyError(name)

    with (
        patch.object(dag_module.Variable, "get", side_effect=get),
        patch.object(dag_module, "conn_kwargs", return_value=CONN),
    ):
        return dag_module.hsm_env(params)


def test_dag_deploys_paused_with_one_run_at_a_time():
    assert _DAG is not None
    assert _DAG.is_paused_upon_creation is True
    assert _DAG.max_active_runs == 1
    assert _DAG.catchup is False


def test_the_image_tag_has_no_default_and_no_credential_is_rendered():
    task = _DAG.get_task("hsm")
    assert task.image.startswith("ghcr.io/thegoodparty/gp-data-platform/hubspot-merge:")
    assert "var.value.hubspot_merge_image_tag" in task.image
    assert task.env_vars == []


@pytest.mark.parametrize(
    ("params", "arguments"),
    [
        ({"command": "run"}, ["run"]),
        ({"command": "dry_run"}, ["run", "--dry-run"]),
        ({"command": "init_log"}, ["init-log"]),
        ({"command": "verify", "run_id": "r1"}, ["verify", "--run-id", "r1"]),
    ],
)
def test_each_command_maps_to_its_hsm_arguments(params, arguments):
    assert dag_module.hsm_arguments(params, scheduled=False) == arguments


def test_a_manual_trigger_defaults_to_a_dry_run_and_a_scheduled_run_merges():
    defaults = {name: param.value for name, param in _DAG.params.items()}
    assert dag_module.hsm_arguments(defaults, scheduled=False) == ["run", "--dry-run"]
    assert dag_module.hsm_arguments(defaults, scheduled=True) == ["run"]


def test_verify_without_a_run_id_fails_before_the_pod_starts():
    with pytest.raises(ValueError, match="run_id"):
        dag_module.hsm_arguments({"command": "verify", "run_id": ""}, scheduled=False)


def test_a_scheduled_run_reads_the_variables():
    env = _env({"command": "run", "pair_relation": "", "max_pairs": None})
    assert env["MERGE_PAIR_RELATION"] == VARIABLES["hubspot_merge_pair_relation"]
    assert env["MERGE_MAX_PAIRS"] == "100"
    assert env["MERGE_MAX_RPS"] == "5"
    assert env["HUBSPOT_TOKEN"] == "pat-test"


def test_trigger_params_override_the_relation_and_the_cap():
    env = _env({"command": "run", "pair_relation": "dbt_dball.five_pairs", "max_pairs": 5})
    assert env["MERGE_PAIR_RELATION"] == "dbt_dball.five_pairs"
    assert env["MERGE_MAX_PAIRS"] == "5"
