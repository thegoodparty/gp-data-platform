"""## Reverse-ETL: HubSpot contacts, daily

One task, `send_pod`, runs the `retl` container (`retl --source=hubspot
--destination=hubspot_contacts`): diff the desired-state contact model against HubSpot,
upsert whatever differs, and log what HubSpot confirmed. The diff, the guards, the batch
upsert, and the send-log append all live inside that container; this DAG only supplies
its environment at task runtime and turns a bare pod failure into one an operator can
act on.

THE DAILY INVOCATION PASSES NEITHER `--init-log` NOR `--accept-empty-log`. Both are
ceremony-only: `--init-log` creates the flow's log table once (or after a deliberate
reset) and exits without running a diff; `--accept-empty-log` is the explicit override
for the one day a real run against a genuinely empty log is expected (the first
convergence, or right after an admin reset). Passing either flag on the daily schedule
would PERMANENTLY DISARM the guard that catches a lost or mis-pointed log table silently
re-sending the full population — do not add them here.

### Schedule contract
`0 17 * * *` (UTC). The scheduled dbt build runs 00:02 and 12:02 UTC and takes 95-105
minutes, so the 12:02 build lands ~13:47; `gold_match_daily` runs 14:30 UTC and its
post-write dbt rebuild can run to ~16:30; the election-api sync runs 00:00 UTC. 17:00
sits ~3h after the 12:02 build's worst-case completion, after gold-match's rebuild
window, and 7h before the 00:00 sync — 13:00/12:00 US Eastern, so a failure alert reaches
a human the same working day and this task's own retries still finish inside that
margin. There is deliberately NO dbt gate: a stale mart is self-correcting (yesterday's
payloads are already logged, so the diff comes up empty), so do not add one.

The DAG deploys `is_paused_upon_creation=True`. Unpausing is the owner-gated enable step.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M, selected by the
  `databricks_conn_id` Variable; shared with the other DAGs.

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects the Databricks connection.
- `reverse_etl_image_tag` — REQUIRED, no default: the merged build's sha. Unset fails
  the task at render, before any pod runs; `latest` works only as an explicit,
  provenance-warned override.
- `reverse_etl_image_pull_secret` — image pull secret name from Astronomer support (the
  GHCR package is private). Empty means the pod pulls anonymously.
- `reverse_etl_hubspot_token` — HubSpot private-app token ("token" in the name so the
  secrets masker redacts it in logs).
- `reverse_etl_hubspot_source_relation` — the desired-state model's relation name.
- `reverse_etl_hubspot_excluded_columns` — comma-separated columns the payload
  never carries (must include the model's build-clock column).
- `reverse_etl_hubspot_cap` — the flow's send-cap, sized at enable time.
- `reverse_etl_hubspot_log_table` — this flow's own send-log table. No default:
  a default would point a dev deployment at the production log.
"""

from __future__ import annotations

import logging
import re
import threading
from collections import deque
from datetime import datetime
from typing import Any

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Variable, dag
from airflow.sdk.exceptions import AirflowException, AirflowSkipException
from include.custom_functions.databricks_utils import conn_kwargs, get_databricks_connection
from kubernetes.client import models as k8s
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

# The tag CI publishes beside `latest` on every merge to main; a digest reference
# (`@sha256:...`) counts as pinned too.
_PINNED_TAG = re.compile(r"[0-9a-f]{40}")

FLOW_ID = "hubspot"
DESTINATION = "hubspot_contacts"
# The spec pins the key; it is not sized or renamed per deployment, so it is a code
# constant rather than a Variable.
KEY_COLUMN = "gp_person_id"
# RETL_FLOW_<FLOW_ID>_* is retl's own config namespace (config.py); deriving it once
# here means the five per-flow env names below can't drift from each other.
_FLOW_ENV_PREFIX = f"RETL_FLOW_{FLOW_ID.upper()}_"

IMAGE_TAG_VARIABLE = "reverse_etl_image_tag"
# REQUIRED, no default (gold-match policy): an unattended loop on a mutable tag would
# silently run whatever main last published; the evaluated artifact must be the
# production artifact. `var.value.X` (no `.get`) fails the render if unset.
REVERSE_ETL_IMAGE = (
    f"ghcr.io/thegoodparty/gp-data-platform/reverse-etl:{{{{ var.value.{IMAGE_TAG_VARIABLE} }}}}"
)
IMAGE_PULL_SECRET_VARIABLE = "reverse_etl_image_pull_secret"
HUBSPOT_TOKEN_VARIABLE = "reverse_etl_hubspot_token"
SOURCE_RELATION_VARIABLE = f"reverse_etl_{FLOW_ID}_source_relation"
EXCLUDED_COLUMNS_VARIABLE = f"reverse_etl_{FLOW_ID}_excluded_columns"
CAP_VARIABLE = f"reverse_etl_{FLOW_ID}_cap"
LOG_TABLE_VARIABLE = f"reverse_etl_{FLOW_ID}_log_table"

# A private multi-hundred-MB image with imagePullPolicy=Always on a cold node can
# exceed the 120s default; the real bound on the run is execution_timeout below.
STARTUP_TIMEOUT_SECONDS = 600
# Bounds a hung pod. Worst case (3 attempts, the retry_delay between them) ends
# ~23:20 UTC, still before the 00:00 election-api sync.
SEND_EXECUTION_TIMEOUT = duration(hours=2)

# Last N pod-log lines to mirror into a failure alert, and the character cap on the
# joined text (from the end) -- a chatty pod must not produce an unusable alert.
POD_LOG_TAIL_LINES = 40
POD_LOG_TAIL_CHARS = 4000

# A diagnostic annotating an already-failed run must never delay it. Retry count alone
# does not bound that: the connector's own socket timeout and internal retry budget are
# both 900s, so even one connect attempt can hang for 15 minutes. One attempt, and a
# hard wall-clock bound on the whole probe.
PROBE_MAX_RETRIES = 1
PROBE_TIMEOUT_SECONDS = 30


def _reverse_etl_pod_env() -> dict[str, str]:
    """The env retl's container reads, resolved from Airflow Variables/Connections at
    task runtime -- module-level so tests can patch it directly.

    Exactly `.env.example`'s surface minus the three variables this deployment's auth
    shape and destination make irrelevant: DATABRICKS_TOKEN (the OAuth client id/secret
    pair is used instead), RETL_HUBSPOT_BASE_URL (defaults to the production portal),
    and RETL_CSV_OUTPUT_PATH (the other destination). Passing an env var retl ignores
    would be false documentation of what this deployment needs.
    """
    fields = conn_kwargs()
    return {
        "DATABRICKS_HOST": fields["host"],
        "DATABRICKS_HTTP_PATH": fields["http_path"],
        "DATABRICKS_CLIENT_ID": fields["client_id"],
        "DATABRICKS_CLIENT_SECRET": fields["client_secret"],
        # From the connection, so the pod cannot drift from the tasks beside it, and
        # always sent so the pod's env surface does not vary with a Variable's state.
        "DATABRICKS_SCOPES": ", ".join(fields["scopes"] or []),
        _FLOW_ENV_PREFIX + "SOURCE_RELATION": Variable.get(SOURCE_RELATION_VARIABLE),
        _FLOW_ENV_PREFIX + "KEY_COLUMN": KEY_COLUMN,
        _FLOW_ENV_PREFIX + "EXCLUDED_COLUMNS": Variable.get(EXCLUDED_COLUMNS_VARIABLE),
        _FLOW_ENV_PREFIX + "CAP": Variable.get(CAP_VARIABLE),
        _FLOW_ENV_PREFIX + "LOG_TABLE": Variable.get(LOG_TABLE_VARIABLE),
        "RETL_HUBSPOT_TOKEN": Variable.get(HUBSPOT_TOKEN_VARIABLE),
    }


def _rows_logged_since(log_table: str, since: datetime) -> int:
    """How many rows `log_table` gained since `since` -- this run's OWN diagnostic
    probe, never retl's. Opened with a short retry budget: a diagnostic annotating an
    already-failed run must not itself burn the connector's ~10-minute cold-start loop.

    `since` binds as a named parameter, matching the binding style retl's own
    `append_sent_log` uses; `log_table` is trusted config (from an Airflow Variable),
    interpolated directly since DB-API params cannot bind a table name.
    """
    connection = get_databricks_connection(
        **conn_kwargs(),
        max_retries=PROBE_MAX_RETRIES,
        use_cloud_fetch=False,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"select count(*) from {log_table} where sent_at >= :since", {"since": since})
            row = cursor.fetchone()
            return int(row[0]) if row else 0
    finally:
        connection.close()


def _call_with_timeout(func, *args, timeout: float):
    """Run `func` on a daemon thread and give up on it after `timeout` seconds.

    A daemon thread, not a pool: a worker blocked on a socket must not hold the task
    process open at exit, and a future that is already running cannot be cancelled.
    Abandoning the thread is the point -- the caller has a failure to report now.
    """
    outcome: dict[str, Any] = {}

    def _run() -> None:
        try:
            outcome["value"] = func(*args)
        except BaseException as exc:
            outcome["error"] = exc

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        raise TimeoutError(f"gave up after {timeout}s")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


class _ReverseEtlPodOperator(KubernetesPodOperator):
    """KPO that resolves its pull secret and pod env at task runtime, and turns a bare
    pod failure into one that carries the pod's own log tail plus how far this run got.

    Credential resolution matches gold-match/matcha: Airflow snapshots rendered
    template fields (env_vars included, plus the whole pod YAML) into the metadata DB
    BEFORE pre_execute runs, and Astro exposes no Variables to the DAG processor at
    parse time -- so resolving here means nothing credential-shaped ever reaches that
    snapshot or the UI.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._log_tail: deque[str] = deque(maxlen=POD_LOG_TAIL_LINES)
        self.log_formatter = self._tee_log_line

    def _tee_log_line(self, container_name: str, message: str) -> str:
        """log_formatter's return value is what reaches the task log, so this must
        return the provider's own default formatting -- teeing here captures the tail
        without changing what an operator sees in the live log."""
        formatted = f"[{container_name}] {message}"
        self._log_tail.append(formatted)
        return formatted

    def pre_execute(self, context) -> None:
        self._log_tail.clear()  # a retry's alert must carry only that attempt's lines
        secret_name = Variable.get(IMAGE_PULL_SECRET_VARIABLE, default="")
        if secret_name:
            self.image_pull_secrets = [k8s.V1LocalObjectReference(name=secret_name)]
        # Assigned, never extended: pre_execute re-runs on every retry.
        self.env_vars = [
            k8s.V1EnvVar(name=name, value=value) for name, value in _reverse_etl_pod_env().items()
        ]
        self._log_image_provenance()
        super().pre_execute(context)

    def _log_image_provenance(self) -> None:
        """A mutable tag means a failure cannot be attributed to the data over a retl
        change; the run's own logs must say which build ran."""
        image = self.image or ""
        _, _, tag = image.rpartition(":")
        if "@sha256:" in image or _PINNED_TAG.fullmatch(tag):
            t_log.info("reverse-etl image pinned for this run: %s", image)
            return
        t_log.warning(
            "reverse-etl image %s is a mutable tag; pin the %s Variable to the sha tag CI "
            "publishes beside `latest` for a reproducible run.",
            image,
            IMAGE_TAG_VARIABLE,
        )

    def _partial_progress_line(self, context) -> str:
        """retl prints its counts only at the end of a run, so a batch failure aborts
        before any of them reach the pod log -- the tail is one `retl FAILED: ...` line
        whether the run died at batch 1 or batch 700. Confirmed batches ARE already
        appended to the flow's log table, so counting rows this run added is the only
        way to tell those two apart. Never allowed to replace the failure it annotates:
        any exception here becomes a line naming itself, not a crash.
        """
        try:
            since = context["dag_run"].start_date
            log_table = Variable.get(LOG_TABLE_VARIABLE)
            count = _call_with_timeout(_rows_logged_since, log_table, since, timeout=PROBE_TIMEOUT_SECONDS)
            return f"partial progress: {count} row(s) logged to {log_table} since this run started ({since})"
        except Exception as exc:
            return f"partial progress probe failed: {type(exc).__name__}: {exc}"

    def cleanup(self, pod, remote_pod, xcom_result=None, context=None) -> None:
        try:
            super().cleanup(pod, remote_pod, xcom_result=xcom_result, context=context)
        except AirflowSkipException:
            # Subclasses AirflowException: catching the parent alone would convert a
            # legitimate skip (skip_on_exit_code) into a failure.
            raise
        except AirflowException as exc:
            tail = "\n".join(self._log_tail)[-POD_LOG_TAIL_CHARS:] or "(the pod printed nothing)"
            raise AirflowException(
                f"{exc}\n\n--- pod log tail (last {len(self._log_tail)} line(s)) ---\n{tail}\n\n"
                f"{self._partial_progress_line(context)}"
            ) from exc


def _send_pod() -> _ReverseEtlPodOperator:
    """The container run: diff the flow's desired state against HubSpot, upsert, log."""
    return _ReverseEtlPodOperator(
        task_id="send_pod",
        name="reverse-etl-hubspot",
        image=REVERSE_ETL_IMAGE,
        image_pull_policy="Always",
        arguments=[f"--source={FLOW_ID}", f"--destination={DESTINATION}"],
        container_resources=k8s.V1ResourceRequirements(
            # ~2x the measured buffered-diff shape (source rows + serialized desired
            # state + latest_sent, plus the connector's own result buffering) with
            # headroom, and a quarter of gold-match's 8Gi. requests == limits is
            # Guaranteed QoS, so an at-least-once sender is not evicted mid-run.
            requests={"memory": "2Gi", "cpu": "1"},
            limits={"memory": "2Gi", "cpu": "1"},
        ),
        startup_timeout_seconds=STARTUP_TIMEOUT_SECONDS,
        in_cluster=True,
        # The three settings the failure alert depends on, all stated rather than
        # inherited. get_logs streams the container log, which is the only thing that
        # calls log_formatter. deferrable defaults to a DEPLOYMENT config lookup, and
        # the deferred path writes logs without the formatter, so a deployment-level
        # default would silently empty every alert. log_pod_spec_on_failure prepends
        # the whole pod object to the exception, burying the tail; the pod's events
        # carry the same diagnosis (OOMKilled, evictions) into the task log instead.
        get_logs=True,
        deferrable=False,
        log_pod_spec_on_failure=False,
        log_events_on_failure=True,
        on_finish_action="delete_pod",
        execution_timeout=SEND_EXECUTION_TIMEOUT,
    )


@dag(
    dag_id="reverse_etl_hubspot",
    schedule="0 17 * * *",
    start_date=pendulum_datetime(2026, 9, 9, tz="UTC"),
    catchup=False,
    # BUILD deploys paused; unpausing is a separate, owner-gated enable step.
    is_paused_upon_creation=True,
    # One writer against the flow's log table at a time.
    max_active_runs=1,
    # The send log checkpoints per confirmed batch, so a retry sends only the
    # remainder rather than re-diffing from scratch.
    default_args={"retries": 2, "retry_delay": duration(minutes=10)},
    tags=["reverse-etl", "hubspot"],
)
def reverse_etl_hubspot():
    _send_pod()


reverse_etl_hubspot()
