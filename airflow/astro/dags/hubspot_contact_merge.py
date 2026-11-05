"""## HubSpot contact merge

One task runs the `hsm` container. A scheduled run merges the pairs in the queue up to the
cap. A manual trigger can choose the command and override the pair relation and the cap,
for hand-checked batches. See `airflow/astro/docs/hubspot_contact_merge.md`.

Deploys paused. Unpausing is the enable step.
"""

from __future__ import annotations

import logging
import re

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Param, Variable, dag
from include.custom_functions.databricks_utils import conn_kwargs
from kubernetes.client import models as k8s
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

IMAGE_TAG_VARIABLE = "hubspot_merge_image_tag"
# No default: an unset tag fails the render rather than run whatever main last published.
IMAGE = f"ghcr.io/thegoodparty/gp-data-platform/hubspot-merge:{{{{ var.value.{IMAGE_TAG_VARIABLE} }}}}"
IMAGE_PULL_SECRET_VARIABLE = "hubspot_merge_image_pull_secret"
TOKEN_VARIABLE = "hubspot_merge_hubspot_token"
PORTAL_VARIABLE = "hubspot_merge_expected_portal_id"
PAIR_RELATION_VARIABLE = "hubspot_merge_pair_relation"
# No default, so a dev deployment cannot write to the production log by omission.
LOG_TABLE_VARIABLE = "hubspot_merge_log_table"
MAX_PAIRS_VARIABLE = "hubspot_merge_max_pairs"
MAX_RPS_VARIABLE = "hubspot_merge_max_rps"
DEFAULT_MAX_RPS = "5"

_PINNED_TAG = re.compile(r"[0-9a-f]{40}")

COMMANDS = {
    "run": ["run"],
    "dry_run": ["run", "--dry-run"],
    "verify": ["verify", "--run-id"],
    "init_log": ["init-log"],
}


def hsm_arguments(params: dict) -> list[str]:
    command = params["command"]
    if command == "verify":
        if not params.get("run_id"):
            raise ValueError("the verify command needs a run_id")
        return [*COMMANDS[command], params["run_id"]]
    return COMMANDS[command]


def hsm_env(params: dict) -> dict[str, str]:
    """The container's environment. A non-empty trigger param wins over its Variable."""
    fields = conn_kwargs()
    return {
        "DATABRICKS_HOST": fields["host"],
        "DATABRICKS_HTTP_PATH": fields["http_path"],
        "DATABRICKS_CLIENT_ID": fields["client_id"],
        "DATABRICKS_CLIENT_SECRET": fields["client_secret"],
        "DATABRICKS_SCOPES": ", ".join(fields["scopes"] or []),
        "HUBSPOT_TOKEN": Variable.get(TOKEN_VARIABLE),
        "HUBSPOT_EXPECTED_PORTAL_ID": Variable.get(PORTAL_VARIABLE),
        "MERGE_OBJECT_TYPE": "contacts",
        "MERGE_PAIR_RELATION": params.get("pair_relation") or Variable.get(PAIR_RELATION_VARIABLE),
        "MERGE_LOG_TABLE": Variable.get(LOG_TABLE_VARIABLE),
        "MERGE_MAX_PAIRS": str(params.get("max_pairs") or Variable.get(MAX_PAIRS_VARIABLE)),
        "MERGE_MAX_RPS": Variable.get(MAX_RPS_VARIABLE, default=DEFAULT_MAX_RPS),
    }


class _HsmPodOperator(KubernetesPodOperator):
    """Resolves the pull secret, the arguments and the env at task runtime, so no
    credential reaches the rendered fields Airflow stores before `pre_execute`."""

    def pre_execute(self, context) -> None:
        params = context["params"]
        secret_name = Variable.get(IMAGE_PULL_SECRET_VARIABLE, default="")
        if secret_name:
            self.image_pull_secrets = [k8s.V1LocalObjectReference(name=secret_name)]
        self.arguments = hsm_arguments(params)
        self.env_vars = [k8s.V1EnvVar(name=name, value=value) for name, value in hsm_env(params).items()]
        tag = (self.image or "").rpartition(":")[2]
        if not _PINNED_TAG.fullmatch(tag):
            t_log.warning("hubspot-merge image %s is not pinned to a commit sha", self.image)
        t_log.info("hsm %s", " ".join(self.arguments))
        super().pre_execute(context)


@dag(
    dag_id="hubspot_contact_merge",
    # After the 12:02 UTC dbt build lands, and before the 17:00 UTC reverse-ETL send.
    schedule="0 15 * * *",
    start_date=pendulum_datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    # One writer against the log at a time.
    max_active_runs=1,
    # A failed run waits for a person. An automatic retry after `id_changed` would merge
    # the next slice of the queue.
    default_args={"retries": 0},
    tags=["hubspot"],
    params={
        "command": Param("run", enum=list(COMMANDS), description="What hsm does."),
        "pair_relation": Param(
            "", type="string", description="Read pairs from this relation instead of the Variable's."
        ),
        "max_pairs": Param(
            None, type=["null", "integer"], minimum=1, description="Cap this run instead of the Variable's."
        ),
        "run_id": Param("", type="string", description="The run to check, for verify."),
    },
)
def hubspot_contact_merge():
    _HsmPodOperator(
        task_id="hsm",
        name="hubspot-contact-merge",
        image=IMAGE,
        image_pull_policy="Always",
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "1Gi", "cpu": "500m"},
            limits={"memory": "1Gi", "cpu": "500m"},
        ),
        startup_timeout_seconds=600,
        in_cluster=True,
        get_logs=True,
        deferrable=False,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
        execution_timeout=duration(hours=3),
    )


hubspot_contact_merge()
