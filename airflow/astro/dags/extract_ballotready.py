"""
## Extract BallotReady (CivicEngine)

Pulls the nine BallotReady GraphQL entities by id and lands their raw node payloads in
Databricks, replacing the direct-API dbt Python models those tables used to come from:

    extract_candidacy       extract_endorsement   extract_filing_period
    extract_geofence         extract_issue         extract_normalized_position
    extract_party            extract_position_election_frequency
    extract_stance

Every task is independent except one: `extract_stance -> extract_issue`, because the issue
worklist reads issue ids out of landed stance payloads (see `issue_worklist_sql`). The other
three Candidacy-keyed entities (`party`, `endorsement`, `stance`) share `candidacy_worklist_sql`
with `extract_candidacy`, and `geofence` builds its own worklist (`geofence_worklist_sql`)
straight off the S3 candidacies feed. None of them wait on `extract_candidacy` — each builds its
own worklist independently rather than from another task's output.

Concurrency against the CivicEngine GraphQL endpoint is bounded by `max_active_tasks` (how many
of the nine tasks can run at once) times each task's own `max_workers` param (how many API calls
that task makes concurrently) — with the defaults below, 4 x 4 = 16 concurrent callers.

### Configuration

Connections: the Databricks connection the `databricks_conn_id` variable names (OAuth M2M SQL
warehouse).

Variables:
- `civicengine_api_token` — bearer token for the CivicEngine GraphQL endpoint.
- `databricks_conn_id` — names the Databricks connection above.
- `databricks_catalog` — the Unity Catalog catalog.
- `databricks_dbt_schema` — holds the `stg_airbyte_source__ballotready_*` dbt models that every
  worklist query reads ids from.
- `databricks_source_schema` — where this DAG's own `ballotready_*_raw` landing tables live
  (`ExtractConfig.source_schema`). The issue worklist reads landed stance/issue rows back out of
  this schema.

### Params

`entities` narrows a manual run to a subset of the nine (empty runs all). An entity not in the
list still runs its task, but as a cheap no-op rather than being removed from the graph — the
task graph is fixed at parse time, and `extract_stance -> extract_issue` must keep working when
only one side is requested (e.g. `entities: ["issue"]` alone still lets `extract_stance` execute,
skip its own work, and hand off cleanly). `full_reload`, `max_ids_per_entity`, `max_workers`, and
`requests_per_second` all forward straight into `ExtractConfig`.
"""

import logging
from datetime import UTC
from datetime import datetime as dt

from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.ballotready_graphql import (
    ENTITY_SPECS,
    ExtractConfig,
    extract_entity,
    should_extract,
)
from include.custom_functions.databricks_utils import connect_from_conn_id
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")


@dag(
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    # Bounds concurrent CivicEngine GraphQL callers (an undocumented rate limit); don't raise
    # without confirming headroom.
    max_active_tasks=4,
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    # Backoff rather than a flat delay: a retry is usually the API rate-limiting us or a
    # transient network failure, and retrying three times 30s apart tends to re-hit whatever
    # is still recovering. Capped so a run cannot stall for hours.
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=10),
    },
    tags=["ballotready", "civicengine", "ingestion"],
    is_paused_upon_creation=True,
    params={
        "full_reload": Param(
            False,
            type="boolean",
            description=(
                "Ignore the cursor and re-sweep. No effect on `issue`: its worklist has no "
                "cursor and always fetches only what is not yet landed."
            ),
        ),
        "max_ids_per_entity": Param(50000, type="integer", minimum=1),
        "entities": Param([], type="array", description="Run only these entities; empty runs all."),
        "max_workers": Param(4, type="integer", minimum=1, maximum=16),
        "requests_per_second": Param(8.0, type="number", minimum=0.1),
    },
)
def extract_ballotready():
    tasks = {}
    for name, spec in ENTITY_SPECS.items():
        # reads_tables means this entity reads another's landed rows rather than that task's
        # return value, so an upstream failure must not strand it (all_success would).
        @task(
            task_id=f"extract_{name}",
            trigger_rule="all_done" if spec.reads_tables else "all_success",
        )
        def _extract(spec=spec) -> dict:
            context = get_current_context()
            params = context["params"]

            if not should_extract(spec.name, params["entities"]):
                t_log.info(
                    f"{spec.name} not in requested entities {sorted(set(params['entities']))}; skipping"
                )
                return {"entity": spec.name, "skipped": True}

            run_id = context["dag_run"].run_id
            # .strip(): an Airflow Variable pasted with a trailing newline or space is a
            # copy-paste error, not an intentional value change, for every one of these.
            config = ExtractConfig(
                catalog=Variable.get("databricks_catalog").strip(),
                dbt_schema=Variable.get("databricks_dbt_schema").strip(),
                source_schema=Variable.get("databricks_source_schema").strip(),
                api_token=Variable.get("civicengine_api_token").strip(),
                max_ids=params["max_ids_per_entity"],
                max_workers=params["max_workers"],
                requests_per_second=params["requests_per_second"],
                full_reload=params["full_reload"],
                dag_run_id=run_id,
                extracted_at=dt.now(UTC).isoformat(),
            )

            connection = connect_from_conn_id()
            try:
                return extract_entity(spec, connection, config)
            finally:
                connection.close()

        tasks[name] = _extract()

    # Ordering only (see the trigger rule above), so an entity that reads another's landing
    # table sees the rows this run landed there.
    for name, spec in ENTITY_SPECS.items():
        for upstream in spec.reads_tables:
            tasks[upstream] >> tasks[name]


extract_ballotready()
