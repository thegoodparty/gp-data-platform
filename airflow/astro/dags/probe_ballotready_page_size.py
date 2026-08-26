"""
## Probe BallotReady page size

TEMPORARY DIAGNOSTIC DAG. Not part of the extraction pipeline, triggered manually a
handful of times to inform one configuration value (a `batch_size` per entity in
`ENTITY_SPECS`), then deleted. Do not schedule it and do not build anything on top of it.

The CivicEngine GraphQL `nodes(ids:)` endpoint is reported to silently drop results
above some undocumented batch size, with no error status. Two shapes are possible:

  a) a SHORT array -> `fetch_nodes` in `ballotready_graphql.py` already catches this at
     runtime (length assertion, bisect, retry).
  b) a FULL-LENGTH array padded with nulls -> nothing catches this today. A null looks
     exactly like "BallotReady genuinely has no such node" to a downstream dbt model.

Shape (b) is why this probe exists. Every entity currently ships at `batch_size=100`
because 100 is the only size proven against the endpoint. The test that matters: take
ids that RESOLVE (come back non-null) at batch 100, re-request the SAME ids at larger
sizes, and check both that the array length holds and that none of those ids comes back
null at the larger size. A null appearing only at the larger size is silent truncation,
not a genuine absence.

### Configuration

Same Variables as `extract_ballotready`: `civicengine_api_token`, `databricks_conn_id`,
`databricks_catalog`, `databricks_staging_schema`, `databricks_intermediate_schema`,
`ballotready_extract_databricks_schema`.

### Params

`entities` narrows the sweep to a subset (empty runs all nine). `max_size` caps how high
the sweep goes, for a cautious first run against a new entity.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import requests
from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.ballotready_graphql import (
    CIVIC_ENGINE_GRAPHQL_URL,
    ENTITY_SPECS,
    EntitySpec,
    RateLimiter,
    chunked,
    encode_node_id,
    is_retryable_status,
    make_session,
    retry_wait_seconds,
)
from include.custom_functions.databricks_utils import connect_from_conn_id, execute_with_retry
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

SIZES = [100, 150, 200, 250, 300, 400, 500]
PROVEN = 100  # the only size proven safe today; also the floor of SIZES
SAFETY_MARGIN = 0.8  # applied to the observed ceiling, since the real limit may be
# response bytes rather than id count, and production payloads for a real run may be
# fatter than whatever happened to be in the sample this probe drew.
PROBE_REQUESTS_PER_SECOND = 5.0  # conservative: this run is trying to find a limit, not lean on one


@dataclass(frozen=True)
class ProbeRow:
    """One (entity, size) measurement, for the results table."""

    size: int
    sent: int
    returned: int | None
    nulls: int | None
    verdict: str


def probe_fetch(
    ids: list[int],
    node_type: str,
    selection: str,
    api_token: str,
    limiter: RateLimiter,
    session: requests.Session,
    timeout: int = 60,
    max_retries: int = 4,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict | None] | None:
    """Fetch `ids` in one `nodes()` call and return whatever came back, short or not.

    `fetch_nodes` in `ballotready_graphql.py` raises the moment `len(nodes) != len(ids)`,
    by design: production must never accept a truncated batch. That is exactly the
    condition this probe is measuring, so it cannot reuse `fetch_nodes` -- raising on a
    short array is the behavior under test, not a bug to route around. This function is
    the "thin request" the task doc calls for: same query shape, same retry/rate-limit
    plumbing, but it returns whatever the API sent (or None if the request could not be
    completed) instead of raising on the array length.
    """
    query = f"query GetNodesBatch($ids: [ID!]!) {{ nodes(ids: $ids) {{ {selection} }} }}"
    payload = {
        "query": query,
        "variables": {"ids": [encode_node_id(node_type, i) for i in ids]},
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
    }

    for attempt in range(max_retries + 1):
        limiter.acquire()
        try:
            response = session.post(CIVIC_ENGINE_GRAPHQL_URL, json=payload, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                t_log.warning("Probe request failed after %d retries: %s", max_retries, exc)
                return None
            sleep(retry_wait_seconds({}, attempt))
            continue

        if is_retryable_status(response.status_code):
            if attempt == max_retries:
                t_log.warning(
                    "CivicEngine returned %s for %d %s ids after %d retries; giving up on this size",
                    response.status_code,
                    len(ids),
                    node_type,
                    max_retries,
                )
                return None
            wait = retry_wait_seconds(response.headers, attempt)
            if response.status_code == 429:
                limiter.pause_for(wait)
            t_log.warning(
                "CivicEngine returned %s for %d %s ids (attempt %d/%d); retrying in %.1fs",
                response.status_code,
                len(ids),
                node_type,
                attempt + 1,
                max_retries,
                wait,
            )
            sleep(wait)
            continue

        if response.status_code != 200:
            t_log.warning(
                "CivicEngine returned %s for %d %s ids; not retryable",
                response.status_code,
                len(ids),
                node_type,
            )
            return None

        body = response.json()
        if body.get("errors"):
            t_log.warning("CivicEngine GraphQL errors for %d %s ids: %s", len(ids), node_type, body["errors"])
            return None
        return (body.get("data") or {}).get("nodes")

    return None


def resolved_at_proven(
    ids: list[int], spec: EntitySpec, api_token: str, limiter: RateLimiter, session: requests.Session
) -> list[int]:
    """Ids that come back non-null in batches of PROVEN. These are the sweep's baseline:
    every id here is known-good at the one size already proven against the endpoint, so
    any null seen for the same id at a larger size is drift, not a genuine miss.
    """
    good: list[int] = []
    for batch in chunked(ids, PROVEN):
        nodes = probe_fetch(batch, spec.node_type, spec.selection, api_token, limiter, session)
        if nodes is None:
            continue
        if len(nodes) != len(batch):
            t_log.warning(
                "%s: even %d ids came back as %d nodes at the proven size of %d -- "
                "100 may not be safe for this entity either",
                spec.name,
                len(batch),
                len(nodes),
                PROVEN,
            )
        for requested_id, node in zip(batch, nodes, strict=False):
            if node is not None:
                good.append(requested_id)
    return good


def sweep_entity(
    spec: EntitySpec, pool: list[int], sizes: list[int], api_token: str, limiter: RateLimiter, session
) -> tuple[list[ProbeRow], int]:
    """Run one entity's sweep, stopping at the first failure. Returns the row log plus
    the recommended `batch_size` (observed ceiling with a safety margin applied).
    """
    baseline = resolved_at_proven(pool[: max(sizes)], spec, api_token, limiter, session)
    if len(baseline) < PROVEN:
        t_log.info(
            "%s: only %d ids resolve at the proven size of %d (wanted %d); "
            "skipping the sweep, keeping the default batch_size",
            spec.name,
            len(baseline),
            PROVEN,
            PROVEN,
        )
        return [], PROVEN

    rows: list[ProbeRow] = []
    best = PROVEN
    for size in sizes:
        if size > len(baseline):
            t_log.info(
                "%s: only %d baseline ids, stopping sweep before size %d", spec.name, len(baseline), size
            )
            break
        batch = baseline[:size]
        nodes = probe_fetch(batch, spec.node_type, spec.selection, api_token, limiter, session)
        if nodes is None:
            rows.append(ProbeRow(size=size, sent=len(batch), returned=None, nulls=None, verdict="ERROR"))
            break
        short = len(nodes) != len(batch)
        nulls = sum(1 for n in nodes if n is None)
        ok = not short and nulls == 0
        verdict = "ok" if ok else ("SHORT ARRAY" if short else "NULL DRIFT")
        rows.append(ProbeRow(size=size, sent=len(batch), returned=len(nodes), nulls=nulls, verdict=verdict))
        if not ok:
            break
        best = size

    margin = PROVEN if best == PROVEN else int(best * SAFETY_MARGIN)
    return rows, margin


def _entity_id_pool_sql(
    entity: str,
    spec: EntitySpec,
    catalog: str,
    staging_schema: str,
    intermediate_schema: str,
    source_schema: str,
    limit: int,
) -> str:
    """The worklist SQL to draw a pool of real, oldest-first ids for `entity`.

    `issue`'s real worklist (`issue_worklist_sql`) reads landed ids out of this DAG's own
    landing table, which is empty until `extract_ballotready` has actually run -- before
    that it would fail with TABLE_OR_VIEW_NOT_FOUND rather than just returning few rows.
    Sourcing straight from the raw airbyte staging table sidesteps that and is simpler
    than standing up a fake landing table just for this probe.
    """
    if entity == "issue":
        table = f"`{catalog}`.`{staging_schema}`.`stg_airbyte_source__ballotready_api_issue`"
        return (
            f"SELECT databaseid AS source_id FROM {table} "
            f"WHERE databaseid IS NOT NULL ORDER BY databaseid ASC LIMIT {int(limit)}"
        )
    return spec.worklist_sql(
        catalog,
        staging_schema,
        intermediate_schema=intermediate_schema,
        source_schema=source_schema,
        after_changed_at=None,
        after_source_id=None,
        limit=limit,
    )


def _log_results_table(entity: str, rows: list[ProbeRow], recommended: int) -> None:
    header = f"{'size':>6} {'sent':>6} {'returned':>9} {'nulls':>6}  verdict"
    lines = [f"{entity} page-size sweep:", header, "-" * len(header)]
    for row in rows:
        returned = "-" if row.returned is None else str(row.returned)
        nulls = "-" if row.nulls is None else str(row.nulls)
        lines.append(f"{row.size:>6} {row.sent:>6} {returned:>9} {nulls:>6}  {row.verdict}")
    lines.append(f"recommended batch_size for {entity}: {recommended}")
    t_log.info("\n".join(lines))


@dag(
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={"retries": 2, "retry_delay": duration(seconds=30)},
    tags=["ballotready", "diagnostic", "temporary"],
    is_paused_upon_creation=True,
    description="TEMPORARY diagnostic probe for the CivicEngine nodes() page-size ceiling. Delete before merge.",
    params={
        "entities": Param([], type="array", description="Sweep only these entities; empty sweeps all nine."),
        "max_size": Param(
            500, type="integer", minimum=PROVEN, maximum=2000, description="Cap the sweep size."
        ),
    },
)
def probe_ballotready_page_size():
    @task
    def probe() -> dict:
        context = get_current_context()
        params = context["params"]

        requested = set(params["entities"])
        unknown = requested - set(ENTITY_SPECS)
        if unknown:
            raise ValueError(f"Unknown entities in `entities` param: {sorted(unknown)}")
        wanted = sorted(requested) if requested else sorted(ENTITY_SPECS)

        sizes = [s for s in SIZES if s <= params["max_size"]]
        if not sizes:
            raise ValueError(f"max_size={params['max_size']} excludes even the proven size of {PROVEN}")
        pool_limit = max(sizes) * 3  # buffer, since not every drawn id is guaranteed to resolve

        catalog = Variable.get("databricks_catalog")
        staging_schema = Variable.get("databricks_staging_schema")
        intermediate_schema = Variable.get("databricks_intermediate_schema")
        source_schema = Variable.get("ballotready_extract_databricks_schema")
        api_token = Variable.get("civicengine_api_token")

        connection = connect_from_conn_id()
        try:
            id_pools: dict[str, list[int]] = {}
            for entity in wanted:
                spec = ENTITY_SPECS[entity]
                sql = _entity_id_pool_sql(
                    entity, spec, catalog, staging_schema, intermediate_schema, source_schema, pool_limit
                )
                cursor = connection.cursor()
                try:
                    execute_with_retry(cursor, sql)
                    id_pools[entity] = [int(row[0]) for row in cursor.fetchall() if row[0] is not None]
                finally:
                    cursor.close()
        finally:
            connection.close()

        limiter = RateLimiter(PROBE_REQUESTS_PER_SECOND)
        recommended: dict[str, int] = {}
        with make_session(1) as session:
            for entity in wanted:
                spec = ENTITY_SPECS[entity]
                pool = id_pools.get(entity, [])
                if not pool:
                    t_log.info("%s: no ids found for the sweep pool; skipping", entity)
                    continue

                rows, margin = sweep_entity(spec, pool, sizes, api_token, limiter, session)
                recommended[entity] = margin
                if rows:
                    _log_results_table(entity, rows, margin)
                else:
                    t_log.info("%s: recommended batch_size %d (sweep skipped, see log above)", entity, margin)

        t_log.info("Recommended batch_size per entity: %s", recommended)
        return recommended

    probe()


probe_ballotready_page_size()
