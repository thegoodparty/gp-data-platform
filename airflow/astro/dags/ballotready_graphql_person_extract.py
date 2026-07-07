"""
## BallotReady GraphQL Person extraction

Extracts CivicEngine (BallotReady) GraphQL Person objects and lands them raw in
S3 as newline-delimited JSON. S3 is the source of truth; downstream loading
(dbt/Airbyte) rebuilds Databricks. The DAG never writes Databricks — it only
reads staging to decide which persons to fetch.

The `people` query can't filter by created/updated time, so persons are fetched
by id via `nodes(ids:)`. A keyset cursor `(source_changed_at, br_person_id)` is
kept in S3, where `source_changed_at` is the greatest created/updated timestamp
across a person's BallotReady candidacy and office-holder rows. Each run pulls
the next `max_persons` ids past the cursor, writes them to S3, then advances the
cursor. The first run sweeps oldest→newest in `max_persons` batches; once caught
up it only pulls newly-changed persons. A person edited without any
candidacy/office-holder change is picked up on the next feed refresh or via
`full_reload`.

This is the Person extraction; other BallotReady GraphQL objects (Race,
Election, OfficeHolder) will land in their own DAGs.

### Parameters
- `full_reload` — ignore the cursor and re-sweep the entire universe.
- `max_persons` — ids fetched per run. Run until a run reports
  `persons_requested < max_persons` to finish the initial sweep.

### Connections / Variables (Astro Environment Manager)
- `databricks_conn_id` (Variable) → a Databricks OAuth M2M connection.
- AWS connection named by `ballotready_graphql_aws_conn_id` (default
  `aws_default`) with write access to the S3 archive.
- Variables: `databricks_catalog`, `databricks_dbt_schema`,
  `civicengine_api_token`, `ballotready_graphql_s3_bucket`,
  `ballotready_graphql_s3_prefix` (default `ballotready/graphql/persons`).
"""

import logging

from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task
from include.custom_functions.ballotready_graphql import (
    chunked,
    fetch_person_batch,
    format_cursor_ts,
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


@dag(
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
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
def ballotready_graphql_person_extract():
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

        # Advance the cursor to the last planned id (even if it had no Person) so
        # ids without a Person are not re-queried forever.
        last_id, last_changed_at = id_rows[-1]
        write_cursor(
            bucket,
            cursor_key,
            aws_conn_id,
            source_changed_at=format_cursor_ts(last_changed_at),
            br_person_id=last_id,
            dag_run_id=dag_run_id,
        )

        summary = {
            "persons_requested": len(person_ids),
            "persons_written": persons_written,
            "batches": batches,
            "cursor_source_changed_at": format_cursor_ts(last_changed_at),
            "cursor_br_person_id": last_id,
        }
        t_log.info("Extraction complete: %s", summary)
        return summary

    extract_persons()


ballotready_graphql_person_extract()
