"""## Data pipeline reader: asks each tool what its pipelines did, sends it to Grafana

Every five minutes this DAG loads the checked-in pipeline list, asks each tool for
the runs of the pipelines it names, and sends one short record per finished run
plus one status update per pipeline to Grafana over OTLP/HTTP as logs. It only
reads: it triggers nothing, reruns nothing, cancels nothing and pauses nothing.
Diagnosis and the Grafana rules are later tickets, and the Airflow and Airbyte
collectors are the next two pull requests on this same DAG.

**The one fact that trips people up:** a dbt Cloud step artifact must be fetched
with `?step=N`. Without the step parameter the API returns the LAST step's
artifact, and on a multi-step job the last step is often `docs generate`, whose
`run_results.json` looks green while the build is red. The step number comes from
the run's own steps; `include/reader/dbt_cloud.py` refuses to fetch without one.

**A manual run does not clear the failure flag.** The pipeline list says the
nightly's real success is a `scheduled` run, because a hand-triggered run may be a
partial or targeted build. The cost is that a hand-fixed nightly's alert stays open
until the next scheduled run, at most 12 hours later. That is the deliberate price
of never letting a partial rerun read as recovery.

### Tasks
- `load_pipelines`: the list, validated. A malformed entry fails here, before
  anything is read.
- `collect_dbt_cloud`: one task per tool, so one tool's outage cannot take the
  reader down. It returns `read_ok = False` on failure rather than raising, and a
  tool that failed still gets a status update with `last_read_ok = 0`.
- `send_to_grafana`: `all_done`, so even an unexpected exception upstream still
  sends a status update. A Grafana failure fails the run, which is what lets rule 4
  see the reader go quiet.
- `save_state`: default `all_success`, so it does not run when the send failed, and
  it does not advance a tool's cursor when that tool's read failed.

### Connections (Astro Environment Manager, by id only; never inspected here)
- `dbt_cloud_reader`: dbt Cloud read-only service token, on the cell host.
- `grafana_otlp`: Grafana Cloud OTLP endpoint, instance id as login, logs-write
  token as password.
- `astro_api_reader`: the Airflow API. Unused in this PR; created now so PR 2 does
  not wait on it.

### Variable
- `data_pipeline_reader_state`: the cursors, the bounded dedupe set, the per-pipeline
  streak and the last good read time. Written at the end of a successful run.
  Nothing secret goes in it.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from airflow.sdk import BaseHook, Variable, dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from include.pipelines.models import Pipeline
from include.pipelines.registry import load_pipelines_from
from include.reader.collect_dbt import collect_tool
from include.reader.dbt_cloud import DbtCloudClient
from include.reader.grafana import SendFailed, send_logs
from include.reader.records import assemble_report
from include.reader.state import apply_collector_payload, load_state
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

PIPELINE_LIST = Path(__file__).resolve().parents[1] / "include" / "pipelines" / "pipelines.yaml"
DBT_CLOUD_CONN_ID = "dbt_cloud_reader"
GRAFANA_CONN_ID = "grafana_otlp"
# The OTLP/HTTP logs path on the instance's OTLP host, from the connection.
LOGS_PATH = "/otlp/v1/logs"


def _pipeline_list() -> list[Pipeline]:
    """Read at task time, so a list edit reaches the next run without a redeploy."""
    return load_pipelines_from(PIPELINE_LIST)


def grafana_endpoint(host: str) -> str:
    """The logs URL from a connection host, which may or may not carry a scheme."""
    base = host.strip().rstrip("/")
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    return base if base.endswith(LOGS_PATH) else base + LOGS_PATH


@dag(
    dag_id="data_pipeline_reader",
    # Every five minutes: the reader watches the nightly rather than triggering it,
    # so its cadence is the resolution rule 1 and rule 4 get.
    schedule=CronTriggerTimetable("*/5 * * * *", timezone="UTC"),
    start_date=pendulum_datetime(2026, 11, 18, tz="UTC"),
    catchup=False,
    # The state write assumes no overlap, so two reader runs must never share it.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": duration(minutes=1)},
    tags=["data-pipeline-reader", "monitoring"],
)
def data_pipeline_reader():
    @task
    def load_pipelines() -> int:
        """Validate the list before anything is read, so a typo fails at the top."""
        pipelines = _pipeline_list()
        t_log.info(
            "reader: watching %d pipeline(s): %s", len(pipelines), ", ".join(p.pipeline for p in pipelines)
        )
        return len(pipelines)

    @task
    def collect_dbt_cloud() -> dict:
        """One task per tool. A failure here is read_ok = False, never a raise."""
        pipelines = [p for p in _pipeline_list() if p.tool == "dbt_cloud"]
        try:
            client = DbtCloudClient.from_conn_id(DBT_CLOUD_CONN_ID)
        except Exception as exc:
            # A host mismatch or a connection that does not resolve. The status
            # update then says last_read_ok = 0; nothing else may fail with it.
            t_log.warning("reader: dbt Cloud client unavailable (%s)", exc.__class__.__name__)
            return {
                "tool": "dbt_cloud",
                "read_ok": False,
                "pipelines": {p.pipeline: {"read_ok": False, "records": []} for p in pipelines},
            }
        return collect_tool(pipelines, client, load_state(Variable.get), datetime.now(UTC))

    @task(trigger_rule="all_done")
    def send_to_grafana(dbt_cloud: dict | None = None) -> int:
        """Build and send every line. A send failure fails the run, which is what
        lets rule 4 see the reader go quiet; save_state then does not run."""
        now = datetime.now(UTC)
        records, statuses = assemble_report(
            _pipeline_list(), {"dbt_cloud": dbt_cloud}, load_state(Variable.get), now
        )
        liveness = {
            "record_type": "reader_liveness",
            "pipeline": "",
            "tool": "",
            "status": "ok",
            "read_at": int(now.timestamp()),
        }
        grafana = BaseHook.get_connection(GRAFANA_CONN_ID)
        try:
            send_logs(
                grafana_endpoint(grafana.host or ""),
                grafana.login or "",
                grafana.password or "",
                [liveness, *records, *statuses],
                now=now,
            )
        except SendFailed:
            # Logged without the response body, which could quote a line we sent.
            t_log.error("reader: the Grafana send failed, so this run fails and rule 4 sees it")
            raise
        t_log.info("reader: sent %d record(s) and %d status update(s)", len(records), len(statuses))
        return len(records) + len(statuses)

    @task
    def save_state(collector: dict, sent: int) -> None:
        """Persist the cursor, dedupe set and streak, only for a tool that read OK.

        Runs on the default all_success, so a failed send leaves state untouched and
        the next run replays the window. A failed read freezes that tool's cursor and
        last_read_at, which is what makes rule 4 fire on a revoked credential.
        """
        now = datetime.now(UTC)
        state = load_state(Variable.get)
        tool_payload = collector or {}
        per_pipeline = tool_payload.get("pipelines") or {}
        for pipeline in _pipeline_list():
            apply_collector_payload(state, pipeline, per_pipeline.get(pipeline.pipeline), now)
        save_state(Variable.set, state)
        t_log.info("reader: state saved after sending %d line(s)", sent)

    loaded = load_pipelines()
    collector = collect_dbt_cloud()
    sent = send_to_grafana(collector)
    loaded >> collector
    save_state(collector, sent)


data_pipeline_reader()
