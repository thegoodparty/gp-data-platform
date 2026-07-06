"""
## BallotReady GraphQL Person Extraction

Extracts CivicEngine (BallotReady) GraphQL Person objects and lands them raw in
S3 as newline-delimited JSON. S3 is the durable source of truth — downstream
loading (dbt/Airbyte) rebuilds Databricks from these files, and the DAG never
writes to Databricks (it only reads staging to decide what to fetch).

### How it decides what to fetch

The CivicEngine `people` query cannot filter by created/updated time, so persons
are fetched by id via `nodes(ids:)`. Freshness is driven by the BallotReady S3
feeds, which carry BR's own created/updated timestamps:

- `source_changed_at` = greatest created/updated across a person's candidacy and
  office-holder rows (`stg_airbyte_source__ballotready_s3_candidacies_v3`,
  `..._office_holders_v3`).
- A keyset cursor `(source_changed_at, br_person_id)` is persisted in S3. Each
  run pulls the next `max_persons` ids past the cursor, fetches them, writes
  NDJSON to S3, then advances the cursor.

The first run (no cursor) sweeps the universe oldest→newest in `max_persons`
batches; once caught up it only pulls newly-changed persons, so it is safe to run
at any time. A person edited without any candidacy/office-holder change is picked
A person edited without any candidacy/office-holder change is picked
up on BR's next feed refresh or via `full_reload`.

### Parameters
- `full_reload` — ignore the cursor and re-sweep the entire universe.
- `max_persons` — cap ids fetched per run (backfill batching). Run repeatedly
  until a run reports `fetched < max_persons` to complete the initial sweep.

### Connections (set in Astro Environment Manager)
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M credentials
  (host, login=client_id, password=client_secret, extra.http_path)
- AWS connection named by the `ballotready_graphql_aws_conn_id` Variable
  (default `aws_default`) — write access to the raw S3 archive

### Variables (set in Astro Environment Manager)
- `databricks_conn_id` — Databricks connection id (`databricks_dev` in dev)
- `databricks_catalog` — e.g. `goodparty_data_catalog`
- `databricks_dbt_schema` — schema where dbt staging lives (`dbt` in prod)
- `civicengine_api_token` — CivicEngine GraphQL bearer token (masked)
- `ballotready_graphql_s3_bucket` — S3 bucket for the raw archive
- `ballotready_graphql_s3_prefix` — key prefix (default `ballotready/graphql/persons`)
- `ballotready_graphql_aws_conn_id` — AWS connection id (default `aws_default`)
"""

import logging
from datetime import UTC

from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task
from include.custom_functions.ballotready_graphql import (
    chunked,
    fetch_person_batch,
    get_person_ids_to_fetch,
    read_cursor,
    write_cursor,
    write_persons_ndjson,
)
from include.custom_functions.databricks_utils import get_databricks_connection
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

DEFAULT_S3_PREFIX = "ballotready/graphql/persons"
FETCH_BATCH_SIZE = 100  # persons per GraphQL nodes() call


def _format_cursor_ts(value) -> str:
    """Render a Databricks timestamp as a plain (tz-naive, UTC) ISO string for the cursor."""
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.isoformat(sep=" ")


@dag(
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    catchup=False,
    default_args={
        "owner": "Data Engineering Team",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["ballotready", "graphql", "person", "ingestion"],
    is_paused_upon_creation=True,
    params={
        "full_reload": Param(
            False,
            type="boolean",
            description="Ignore the S3 cursor and re-sweep the entire person universe.",
        ),
        "max_persons": Param(
            25000,
            type="integer",
            minimum=1,
            description="Max persons to fetch this run (backfill batching).",
        ),
    },
)
def ballotready_graphql_extract():
    @task
    def extract_persons() -> dict:
        """Fetch the next batch of new/changed persons and write them to S3."""
        context = get_current_context()
        params = context["params"]
        full_reload = params.get("full_reload", False)
        max_persons = params["max_persons"]

        dag_run = context["dag_run"]
        dag_run_id = dag_run.run_id
        run_key = dag_run_id.replace(":", "").replace("+", "_").replace(" ", "_")
        run_dt = dag_run.logical_date or dag_run.run_after
        ingested_date = run_dt.strftime("%Y-%m-%d")
        extracted_at = run_dt.isoformat()

        catalog = Variable.get("databricks_catalog")
        dbt_schema = Variable.get("databricks_dbt_schema")
        api_token = Variable.get("civicengine_api_token")
        bucket = Variable.get("ballotready_graphql_s3_bucket")
        prefix = Variable.get("ballotready_graphql_s3_prefix", default=DEFAULT_S3_PREFIX)
        aws_conn_id = Variable.get("ballotready_graphql_aws_conn_id", default="aws_default")
        cursor_key = f"{prefix}/_state/cursor.json"

        # Resolve the keyset cursor and the next batch of ids to fetch.
        cursor_ts, cursor_id = (None, None)
        if not full_reload:
            cursor_ts, cursor_id = read_cursor(bucket, cursor_key, aws_conn_id)

        db_conn_id = Variable.get("databricks_conn_id")
        db_conn = BaseHook.get_connection(db_conn_id)
        connection = get_databricks_connection(
            host=db_conn.host,
            http_path=db_conn.extra_dejson.get("http_path", ""),
            client_id=db_conn.login,
            client_secret=db_conn.password,
        )
        try:
            id_rows = get_person_ids_to_fetch(
                connection,
                catalog,
                dbt_schema,
                after_changed_at=cursor_ts,
                after_person_id=cursor_id,
                limit=max_persons,
            )
        finally:
            connection.close()

        if not id_rows:
            t_log.info("No new or changed persons past the cursor — caught up.")
            return {"persons_requested": 0, "persons_written": 0, "batches": 0}

        person_ids = [pid for pid, _ in id_rows]
        t_log.info(
            "Fetching %d persons (cursor: %s / %s)%s",
            len(person_ids),
            cursor_ts,
            cursor_id,
            " [full reload]" if full_reload else "",
        )

        persons_written = 0
        batches = 0
        for batch_ids in chunked(person_ids, FETCH_BATCH_SIZE):
            persons = fetch_person_batch(batch_ids, api_token)
            key = f"{prefix}/data/ingested_date={ingested_date}/{run_key}/part-{batches:05d}.json"
            persons_written += write_persons_ndjson(
                bucket, key, aws_conn_id, persons, extracted_at, dag_run_id
            )
            batches += 1
            t_log.info(
                "Batch %d: requested %d, wrote %d persons to s3://%s/%s",
                batches,
                len(batch_ids),
                len(persons),
                bucket,
                key,
            )

        # Advance the cursor to the last planned id (whether or not it resolved to
        # a Person) so ids without a Person are not re-queried forever.
        last_id, last_changed_at = id_rows[-1]
        write_cursor(
            bucket,
            cursor_key,
            aws_conn_id,
            source_changed_at=_format_cursor_ts(last_changed_at),
            br_person_id=last_id,
            dag_run_id=dag_run_id,
        )

        summary = {
            "persons_requested": len(person_ids),
            "persons_written": persons_written,
            "batches": batches,
            "cursor_source_changed_at": _format_cursor_ts(last_changed_at),
            "cursor_br_person_id": last_id,
        }
        t_log.info("Extraction complete: %s", summary)
        return summary

    extract_persons()


ballotready_graphql_extract()
