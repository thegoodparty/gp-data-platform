"""## HubSpot companion loader: activity-to-contact associations

Lands each HubSpot activity's linked contact ids into
`<landing schema>.companion_activity_contact_associations`, one complete snapshot per
activity, with an explicit empty array where it has none.

This exists because Lakeflow Connect's activity tables carry no association of any kind.
The single-id fallbacks reach a few percent of rows and `int__user_hubspot_touches`
excludes unassociated activities, so without this table no activity type can cut over:
a Lakeflow-owned call with no association would leave the touch history while the staging
row count stayed flat.

### Configuration

Variables:
- `hubspot_companion_loader_token` — the dedicated loader key. Set by the owner, never
  committed, and read here at task runtime so it never reaches a rendered DAG field.
- `hubspot_companion_landing_schema` — `catalog.schema` the companion tables land in.
  `raw.hubspot` in production, `dev.hubspot` for dev, because `raw` has no `_dev` sibling.
- `hubspot_companion_activity_source` — `catalog.schema` holding the activity tables the
  worklist reads. `goodparty_data_catalog.lakeflow_source_dev` today; it becomes
  `raw.hubspot` at the D3 cutover, where the loader's principal has no SELECT yet. It is a
  Variable rather than a constant so that cutover is a configuration change, and the grant
  is solved as part of the writer question rather than here.
- `databricks_conn_id` — names the Databricks connection.

### Params

`activities` narrows a manual run to a subset of the four types. `full_sweep` ignores the
derived cursor and re-reads every activity, which is what a backfill after a landing
rebuild needs.

### Cadence

Deploys paused, and **carries no schedule yet**: how often the companion re-reads the
activity tables is the owner's cadence call, and it interacts with the dbt build cadence
rather than standing alone. Set `SCHEDULE` here when that call is made; unpausing is the
enable step.
"""

from __future__ import annotations

import logging
from datetime import UTC
from datetime import datetime as dt

from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.hubspot_api import (
    ACTIVITY_OBJECTS,
    AssociationExtractionConfig,
    extract_activity_associations,
)
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

TOKEN_VARIABLE = "hubspot_companion_loader_token"
LANDING_SCHEMA_VARIABLE = "hubspot_companion_landing_schema"
ACTIVITY_SOURCE_VARIABLE = "hubspot_companion_activity_source"

# None until the owner sets a cadence. See the module docstring.
SCHEDULE = None

_ACTIVITY_NAMES = tuple(activity_object for activity_object, _ in ACTIVITY_OBJECTS)


def should_extract(activity_object: str, requested: list[str]) -> bool:
    """Whether this activity type is in the run's `activities` param.

    Raises on an unrecognised name rather than skipping everything, which is what a typo
    in a manual trigger would otherwise look like: a green run that read nothing.
    """
    requested_set = set(requested or [])
    unknown = requested_set - set(_ACTIVITY_NAMES)
    if unknown:
        raise ValueError(f"Unknown activities in the `activities` param: {sorted(unknown)}")
    return not requested_set or activity_object in requested_set


def _split_schema(value: str, variable_name: str) -> tuple[str, str]:
    """Split a `catalog.schema` Variable into its parts, naming it on a malformed value."""
    parts = (value or "").strip().split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"{variable_name} must be `catalog.schema`, got {value!r}")
    return parts[0], parts[1]


@dag(
    dag_id="extract_hubspot_companion",
    schedule=SCHEDULE,
    start_date=datetime(2026, 11, 20, tz="UTC"),
    catchup=False,
    # One writer per landing table, and a concurrent run would double the read load against
    # HubSpot for no gain.
    max_active_runs=1,
    # Bounds concurrent HubSpot readers. Each task issues one request per 1,000 activities,
    # so four at once stays far inside the per-portal rate limit.
    max_active_tasks=len(ACTIVITY_OBJECTS),
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    # Backoff rather than a flat delay: a retry is usually a rate limit or a transient
    # network failure, and a flat short delay tends to re-hit whatever is still recovering.
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=10),
    },
    tags=["hubspot", "companion", "ingestion"],
    is_paused_upon_creation=True,
    params={
        "activities": Param(
            [],
            type="array",
            description=f"Read only these activity types ({', '.join(_ACTIVITY_NAMES)}); empty reads all.",
        ),
        "full_sweep": Param(
            False,
            type="boolean",
            description="Ignore the derived cursor and re-read every activity.",
        ),
    },
)
def extract_hubspot_companion():
    for activity_object, activity_type in ACTIVITY_OBJECTS:

        @task(task_id=f"extract_{activity_object}")
        def _extract(activity_object=activity_object, activity_type=activity_type) -> dict:
            context = get_current_context()
            params = context["params"]

            if not should_extract(activity_object, params["activities"]):
                t_log.info(
                    "%s not in requested activities %s; skipping",
                    activity_object,
                    sorted(set(params["activities"])),
                )
                return {"activity_type": activity_type, "skipped": True}

            catalog, schema = _split_schema(Variable.get(LANDING_SCHEMA_VARIABLE), LANDING_SCHEMA_VARIABLE)
            # .strip() on the token: a Variable pasted with a trailing newline is a
            # copy-paste error, not a value change.
            config = AssociationExtractionConfig(
                catalog=catalog,
                schema=schema,
                source_prefix=Variable.get(ACTIVITY_SOURCE_VARIABLE),
                api_token=Variable.get(TOKEN_VARIABLE).strip(),
                dag_run_id=context["dag_run"].run_id,
                extracted_at=dt.now(UTC).isoformat(),
                full_sweep=params["full_sweep"],
            )

            connection = connect_from_conn_id()
            try:
                return extract_activity_associations(connection, config, activity_object, activity_type)
            finally:
                connection.close()

        _extract()


extract_hubspot_companion()
