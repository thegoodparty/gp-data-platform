"""## Extract the HubSpot Office Holder custom object

Reads the Office Holder custom object (object type `2-65502604`) through the HubSpot CRM API
and lands one complete snapshot per record in `raw.hubspot.companion_office_holders`, with
each record's linked Contact and Company ids. dbt reads it through the `hubspot_companion`
source.

One task, because the landing is one snapshot and a task boundary would only add a partial
state: `extract` sweeps the object, inserts every record, and writes the run's completion
marker last.

### A full sweep, every run

Every run lists every record and reads every record's links. That is deliberate and it is
the one place this differs from an incremental extractor:

- The app upserts a record and then creates its links in separate association calls, so a
  run landing between the two records empty arrays, and a later link change does not move
  the record's `hs_lastmodifieddate`, so a modified-since cursor would never revisit it.
- The model is one row per live record, and a cursor never sees a deletion.

The list endpoint is used rather than search, which stops at 10,000 results.

### A partial run cannot shrink the live set

Rows are append-only under one run id, and the completion marker row in
`companion_office_holders_runs` is written after the last record row. Staging reads only
the newest run that has a marker, so a run that dies halfway leaves the previous complete
snapshot in force. A marker rather than one atomic statement, because the landing spans a
page loop plus one association batch series per direction.

### Connections (set in Astro Environment Manager)

- `databricks_conn_id` (Variable) names a Databricks connection (Generic, OAuth M2M).

### Variables

- `databricks_conn_id` — the Variable naming the Databricks connection.
- `databricks_catalog` — the Unity Catalog catalog.
- `hubspot_companion_schema` — the landing schema. Production lands `raw.hubspot`; a dev or
  CI deployment lands `dev.hubspot`, because `raw` has no `_dev` sibling and one catalog is
  shared, so the schema is what keeps a dev run off production data.
- `hubspot_companion_loader_token` — the read-only loader key. Read at task runtime and
  nowhere else.

### Params

- `dry_run` — sweep the object and log the counts, write nothing.

The DAG deploys `is_paused_upon_creation=True`: unpausing is the owner-gated activation step.

### Schedule

At least daily, since the warehouse copy is expected to be current each day, and the exact
hour is an owner decision rather than something to bake in here. So the cron comes from the
Astro environment variable `HUBSPOT_OFFICE_HOLDERS_SCHEDULE` and falls back to `@daily`; it
is read at parse time because that is when Airflow resolves a schedule.
"""

import logging
import os

from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.hubspot_office_holders import extract_office_holders, make_session
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

TOKEN_VARIABLE = "hubspot_companion_loader_token"
SCHEMA_VARIABLE = "hubspot_companion_schema"

# The cadence is an owner decision, not a property of the loader, so it comes from the
# deployment's environment and defaults to once a day. Read at parse time because that is
# when Airflow resolves a schedule.
SCHEDULE_ENV = "HUBSPOT_OFFICE_HOLDERS_SCHEDULE"
DEFAULT_SCHEDULE = "@daily"
SCHEDULE = os.environ.get(SCHEDULE_ENV, DEFAULT_SCHEDULE).strip() or DEFAULT_SCHEDULE


@dag(
    dag_id="extract_hubspot_office_holders",
    start_date=datetime(2026, 11, 1, tz="UTC"),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    doc_md=__doc__,
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": duration(seconds=60),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=10),
    },
    tags=["hubspot", "ingestion", "companion"],
    params={
        "dry_run": Param(
            False,
            type="boolean",
            description="Sweep the object and log the counts; write nothing.",
        ),
    },
)
def extract_hubspot_office_holders():
    @task
    def extract() -> dict:
        context = get_current_context()
        dry_run = bool(context["params"]["dry_run"])
        run_id = context["dag_run"].run_id

        # A dry run opens no connection, so it needs no Databricks credential: it exists to
        # show what the sweep would land.
        connection = None if dry_run else connect_from_conn_id()

        session = make_session()
        try:
            return extract_office_holders(
                connection,
                session,
                Variable.get(TOKEN_VARIABLE),
                Variable.get("databricks_catalog").strip(),
                Variable.get(SCHEMA_VARIABLE).strip(),
                run_id,
                dry_run=dry_run,
            )
        finally:
            session.close()
            if connection is not None:
                connection.close()

    extract()


extract_hubspot_office_holders()
