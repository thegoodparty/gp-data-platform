"""Extraction helpers for the extract_ballotready DAG.

Pulls BallotReady (CivicEngine) GraphQL objects by id and lands the raw node
payloads in Databricks. Every entity is addressed the same way, through
`nodes(ids:)` over base64 global ids, so one client and one registry cover all
of them.
"""

import gzip
import json
import logging
import random
import re
import threading
import time
from base64 import b64encode
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any

from include.custom_functions.databricks_utils import execute_with_retry

logger = logging.getLogger("airflow.task")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")

CIVIC_ENGINE_GRAPHQL_URL = "https://bpi.civicengine.com/graphql"
_NODE_ID_PREFIX = "gid://ballot-factory"


def encode_node_id(node_type: str, node_id: int) -> str:
    """Encode an integer BallotReady id as its base64 GraphQL global id."""
    return b64encode(f"{_NODE_ID_PREFIX}/{node_type}/{node_id}".encode()).decode("utf-8")


def chunked(seq: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive `size`-length chunks of `seq`."""
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


class RateLimiter:
    """Caps request rate across threads, and lets any thread pause all of them.

    Worker count bounds how much is in flight; this bounds the load we actually
    put on CivicEngine. The pause exists because a 429 handled by one worker
    alone just means the other workers keep earning more of them.
    """

    def __init__(
        self,
        requests_per_second: float,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        if requests_per_second <= 0:
            raise ValueError(f"requests_per_second must be > 0, got {requests_per_second}")
        self._interval = 1.0 / requests_per_second
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        """Block until this thread may issue a request."""
        with self._lock:
            wait = self._next_allowed - self._clock()
            if wait > 0:
                self._sleep(wait)
            self._next_allowed = self._clock() + self._interval

    def pause_for(self, seconds: float) -> None:
        """Hold every worker for `seconds`, after a rate-limit response."""
        with self._lock:
            self._next_allowed = max(self._next_allowed, self._clock() + seconds)


def is_retryable_status(status_code: int) -> bool:
    """429 (rate limited) and 5xx (server) responses are worth retrying."""
    return status_code == 429 or status_code >= 500


def retry_wait_seconds(
    headers: Mapping[str, str],
    attempt: int,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
    rng: Callable[[float, float], float] = random.uniform,
) -> float:
    """Seconds to wait before the next retry.

    Honors a numeric `Retry-After` when present; otherwise exponential backoff
    with full jitter so concurrent workers do not resynchronize on the retry.
    """
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return max(0.0, min(float(retry_after), max_backoff))
        except ValueError:
            pass
    return rng(0, min(base_backoff * (2**attempt), max_backoff))


@dataclass(frozen=True)
class FetchedNode:
    """One requested id and whatever the API returned for it.

    `node` is None when the API returned no node for that id. A row is landed
    either way, so an id that resolves to nothing is never re-requested forever.
    """

    requested_id: int
    node: dict[str, Any] | None


def _build_query(selection: str) -> str:
    return f"query GetNodesBatch($ids: [ID!]!) {{ nodes(ids: $ids) {{ {selection} }} }}"


def fetch_nodes(
    ids: list[int],
    node_type: str,
    selection: str,
    api_token: str,
    limiter: RateLimiter,
    session,
    timeout: int = 60,
    max_retries: int = 5,
    sleep: Callable[[float], None] = time.sleep,
) -> list[FetchedNode]:
    """Fetch `ids` in one nodes() call, mapping results positionally.

    A response shorter than the request is how CivicEngine signals that the page
    was too large; it is not an error status. Bisect and retry rather than
    accept the loss, because the missing rows would land as null payloads that
    are indistinguishable from a genuine absence downstream.
    """
    payload = {
        "query": _build_query(selection),
        "variables": {"ids": [encode_node_id(node_type, i) for i in ids]},
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
    }

    for attempt in range(max_retries + 1):
        limiter.acquire()
        response = session.post(CIVIC_ENGINE_GRAPHQL_URL, json=payload, headers=headers, timeout=timeout)

        if is_retryable_status(response.status_code):
            if attempt == max_retries:
                response.raise_for_status()
            wait = retry_wait_seconds(response.headers, attempt)
            if response.status_code == 429:
                # Hold every worker, not just this one, or the others earn more 429s.
                limiter.pause_for(wait)
            logger.warning(
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

        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"CivicEngine GraphQL errors: {body['errors']}")
        nodes = (body.get("data") or {}).get("nodes") or []

        if len(nodes) != len(ids):
            if len(ids) == 1:
                raise RuntimeError(
                    f"CivicEngine returned {len(nodes)} nodes for 1 id "
                    f"({node_type} {ids[0]}); cannot bisect further"
                )
            midpoint = len(ids) // 2
            logger.warning(
                "CivicEngine returned %d nodes for %d %s ids: page size is above the "
                "server's ceiling. Bisecting to %d.",
                len(nodes),
                len(ids),
                node_type,
                midpoint,
            )
            args = (node_type, selection, api_token, limiter, session, timeout, max_retries, sleep)
            return fetch_nodes(ids[:midpoint], *args) + fetch_nodes(ids[midpoint:], *args)

        return [FetchedNode(requested_id=i, node=n) for i, n in zip(ids, nodes, strict=True)]

    raise RuntimeError("Unreachable: fetch_nodes exhausted retries without returning")


# Selections below are copied verbatim (field-for-field) from the dbt Python
# models this DAG replaces, so their landed payloads keep parity with what
# those models used to read from the API directly.

CANDIDACY_SELECTION = """
... on Candidacy {
    candidate {
        databaseId
    }
    createdAt
    databaseId
    election {
        databaseId
    }
    endorsements {
        databaseId
        id
    }
    id
    isCertified
    isHidden
    parties {
        databaseId
        id
    }
    position {
        databaseId
    }
    race {
        databaseId
    }
    result
    stances {
        databaseId
        id
    }
    updatedAt
    withdrawn
}
"""

ENDORSEMENT_SELECTION = """
... on Candidacy {
    id
    databaseId
    endorsements {
        databaseId
        id
        createdAt
        endorser
        recommendation
        status
        updatedAt
        organization {
            databaseId
            id
        }
    }
}
"""

FILING_PERIOD_SELECTION = """
... on FilingPeriod {
    createdAt
    databaseId
    endOn
    id
    notes
    startOn
    type
    updatedAt
}
"""

GEOFENCE_SELECTION = """
... on Geofence {
    createdAt
    databaseId
    geoId
    id
    mtfcc
    updatedAt
    validFrom
    validTo
}
"""

ISSUE_SELECTION = """
... on Issue {
    databaseId
    # expandedText
    id
    key
    name
    # parentIssue {
    #     databaseId
    #     id
    # }
    pluginEnabled
    responseType
    rowOrder
}
"""

NORMALIZED_POSITION_SELECTION = """
... on NormalizedPosition {
    databaseId
    description
    id
    issues {
        databaseId
        id
    }
    mtfcc
    name
}
"""

PARTY_SELECTION = """
... on Candidacy {
  id
  databaseId
  parties {
    createdAt
    databaseId
    id
    name
    shortName
    updatedAt
  }
}
"""

POSITION_ELECTION_FREQUENCY_SELECTION = """
... on PositionElectionFrequency {
    databaseId
    frequency
    id
    referenceYear
    seats
    validFrom
    validTo
}
"""

STANCE_SELECTION = """
... on Candidacy {
    id
    databaseId
    stances {
        databaseId
        id
        issue {
            databaseId
            id
        }
        locale
        referenceUrl
        statement
    }
}
"""


@dataclass(frozen=True)
class EntitySpec:
    """Everything that differs between the nine entity tasks.

    The task body is identical for all of them; only this differs. Person slots
    in later as a tenth spec with node_type "Candidate".
    """

    name: str
    node_type: str
    selection: str
    batch_size: int
    worklist_sql: Callable[..., str]


def landing_table(catalog: str, schema: str, entity: str) -> str:
    """Fully qualified, backtick-quoted landing table for an entity."""
    return f"`{catalog}`.`{schema}`.`ballotready_{entity}_raw`"


def validate_identifier(name: str, value: str) -> str:
    """Identifiers cannot be bound as parameters in DDL, so they are validated instead."""
    if not _IDENTIFIER_RE.match(value or ""):
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def format_cursor_ts(value: datetime) -> str:
    """Render a Databricks timestamp as a tz-naive UTC literal, keeping sub-second precision.

    Precision matters: the tiebreak is only exact if ties really are ties.
    """
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.isoformat(sep=" ", timespec="microseconds")


def read_cursor(connection, catalog: str, schema: str, entity: str) -> tuple[datetime | None, int | None]:
    """The highest (source_changed_at, requested_id) already landed for this entity.

    Derived from the landing table rather than kept separately, so a run that
    dies part way leaves a cursor that is exactly true.
    """
    table = landing_table(
        validate_identifier("catalog", catalog), validate_identifier("schema", schema), entity
    )
    cursor = connection.cursor()
    try:
        execute_with_retry(
            cursor,
            f"SELECT source_changed_at, requested_id FROM {table} "
            "ORDER BY source_changed_at DESC, requested_id DESC LIMIT 1",
        )
        row = cursor.fetchone()
        return (row[0], int(row[1])) if row else (None, None)
    finally:
        cursor.close()


def _keyset_predicate(after_changed_at: str | None, after_source_id: int | None) -> str:
    """The keyset half of the WHERE clause, or an always-true stand-in."""
    # A cursor is only usable as a pair; if just one half is missing, treat it as
    # no cursor at all (a full sweep) rather than raise, since an over-broad
    # worklist is safe and a raise here would stall a run over a partial value.
    if after_changed_at is None or after_source_id is None:
        return "source_changed_at IS NOT NULL"
    ts = format_cursor_ts(datetime.fromisoformat(after_changed_at))
    sid = int(after_source_id)
    return (
        "source_changed_at IS NOT NULL AND ("
        f"source_changed_at > TIMESTAMP '{ts}' OR "
        f"(source_changed_at = TIMESTAMP '{ts}' AND source_id > {sid}))"
    )


def _worklist(inner_sql: str, predicate: str, limit: int) -> str:
    return (
        f"WITH worklist AS ({inner_sql}) "
        f"SELECT source_id, source_changed_at FROM worklist WHERE {predicate} "
        f"ORDER BY source_changed_at ASC, source_id ASC LIMIT {int(limit)}"
    )


def candidacy_worklist_sql(
    catalog: str,
    staging_schema: str,
    *,
    intermediate_schema: str | None = None,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Candidacy ids from the S3 feed plus the upcoming-race roster.

    The S3 feed omits many upcoming general-stage rosters that the API race
    object already carries, and without them those candidacies are never fetched.
    The two sources live in different schemas (stg_* models vs int_* models),
    so this is the one builder that needs both schema parameters.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("staging_schema", staging_schema)
    if intermediate_schema is None:
        raise ValueError(
            "intermediate_schema is required: the upcoming-ids source lives outside staging_schema"
        )
    validate_identifier("intermediate_schema", intermediate_schema)
    staging_base = f"`{catalog}`.`{staging_schema}`"
    intermediate_base = f"`{catalog}`.`{intermediate_schema}`"
    inner = (
        "SELECT cast(br_candidacy_id AS bigint) AS source_id, "
        "greatest(candidacy_created_at, candidacy_updated_at) AS source_changed_at "
        f"FROM {staging_base}.`stg_airbyte_source__ballotready_s3_candidacies_v3` "
        "WHERE br_candidacy_id IS NOT NULL "
        "UNION ALL "
        "SELECT cast(br_candidacy_id AS bigint) AS source_id, race_updated_at AS source_changed_at "
        f"FROM {intermediate_base}.`int__ballotready_upcoming_candidacy_ids` "
        "WHERE br_candidacy_id IS NOT NULL"
    )
    grouped = (
        f"SELECT source_id, max(source_changed_at) AS source_changed_at FROM ({inner}) GROUP BY source_id"
    )
    return _worklist(grouped, _keyset_predicate(after_changed_at, after_source_id), limit)


def geofence_worklist_sql(
    catalog: str,
    staging_schema: str,
    *,
    intermediate_schema: str | None = None,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Geofence ids referenced by candidacies; geofences carry no update feed of their own.

    Many candidacies share one geofence, so the freshest of them decides when
    that geofence is next due for a refetch.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("staging_schema", staging_schema)
    table = f"`{catalog}`.`{staging_schema}`.`stg_airbyte_source__ballotready_s3_candidacies_v3`"
    inner = (
        "SELECT cast(br_geofence_id AS bigint) AS source_id, candidacy_updated_at AS source_changed_at "
        f"FROM {table} WHERE br_geofence_id IS NOT NULL"
    )
    grouped = (
        f"SELECT source_id, max(source_changed_at) AS source_changed_at FROM ({inner}) GROUP BY source_id"
    )
    return _worklist(grouped, _keyset_predicate(after_changed_at, after_source_id), limit)


def race_derived_worklist_sql(
    catalog: str,
    staging_schema: str,
    *,
    intermediate_schema: str | None = None,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Filing period ids exploded out of each race's `filing_periods` array.

    Many races reference the same filing period, so the freshest referencing
    race decides when that filing period is next due for a refetch.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("staging_schema", staging_schema)
    table = f"`{catalog}`.`{staging_schema}`.`stg_airbyte_source__ballotready_api_race`"
    inner = (
        "SELECT cast(filing_period.databaseId AS bigint) AS source_id, updated_at AS source_changed_at "
        f"FROM {table} LATERAL VIEW explode(filing_periods) AS filing_period "
        "WHERE filing_period.databaseId IS NOT NULL"
    )
    grouped = (
        f"SELECT source_id, max(source_changed_at) AS source_changed_at FROM ({inner}) GROUP BY source_id"
    )
    return _worklist(grouped, _keyset_predicate(after_changed_at, after_source_id), limit)


def position_derived_worklist_sql(
    catalog: str,
    staging_schema: str,
    *,
    field: str,
    intermediate_schema: str | None = None,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Ids derived from one field of the position payload.

    Two entities (normalized positions, position election frequencies) key off
    this one staging model with different shapes, so `field` picks between
    them; a later task binds it per entity with `functools.partial`, leaving
    the call site identical to the other three builders.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("staging_schema", staging_schema)
    table = f"`{catalog}`.`{staging_schema}`.`stg_airbyte_source__ballotready_api_position`"
    if field == "normalized_position":
        id_expr = "normalized_position.databaseId"
        from_clause = table
    elif field == "election_frequencies":
        id_expr = "election_frequency.databaseId"
        from_clause = f"{table} LATERAL VIEW explode(election_frequencies) AS election_frequency"
    else:
        raise ValueError(f"field must be 'normalized_position' or 'election_frequencies', got {field!r}")
    inner = (
        f"SELECT cast({id_expr} AS bigint) AS source_id, updated_at AS source_changed_at "
        f"FROM {from_clause} WHERE {id_expr} IS NOT NULL"
    )
    grouped = (
        f"SELECT source_id, max(source_changed_at) AS source_changed_at FROM ({inner}) GROUP BY source_id"
    )
    return _worklist(grouped, _keyset_predicate(after_changed_at, after_source_id), limit)


def issue_worklist_sql(
    catalog: str,
    staging_schema: str,
    *,
    intermediate_schema: str | None = None,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Issue ids referenced by landed stances that have not been fetched yet.

    Issues have no timestamped feed to key a cursor on, and the set is small and
    slow-changing, so "everything referenced but not yet landed" is both correct
    and cheap. staging_schema, intermediate_schema, and the keyset cursor args
    are accepted but unused, so this builder is callable identically to the rest.
    """
    validate_identifier("catalog", catalog)
    if source_schema is None:
        raise ValueError("source_schema is required: issue ids are read out of the landed stance table")
    validate_identifier("source_schema", source_schema)
    stance = landing_table(catalog, source_schema, "stance")
    issue = landing_table(catalog, source_schema, "issue")
    referenced = (
        "SELECT DISTINCT cast(get_json_object(stance_json, '$.issue.databaseId') AS bigint) AS source_id "
        f"FROM {stance} "
        "LATERAL VIEW explode(from_json(get_json_object(payload, '$.stances'), 'array<string>')) "
        "AS stance_json "
        "WHERE payload IS NOT NULL"
    )
    return (
        f"WITH referenced AS ({referenced}) "
        "SELECT source_id, current_timestamp() AS source_changed_at FROM referenced "
        "WHERE source_id IS NOT NULL AND NOT EXISTS ("
        f"SELECT 1 FROM {issue} landed WHERE landed.requested_id = referenced.source_id"
        ") "
        f"ORDER BY source_id ASC LIMIT {int(limit)}"
    )


def rows_to_ndjson_gz(
    fetched: list[FetchedNode],
    changed_at_by_id: dict[int, datetime],
    extracted_at: str,
    dag_run_id: str,
) -> bytes:
    """Serialize a batch as gzipped NDJSON matching the landing table columns.

    A line is written for every requested id, with a null payload where the API
    returned nothing. Skipping those would leave the id below the cursor forever
    and would read downstream as a deletion rather than an absence.
    """
    if not fetched:
        return b""
    lines = []
    for item in fetched:
        node = item.node
        changed_at = changed_at_by_id.get(item.requested_id)
        lines.append(
            json.dumps(
                {
                    "requested_id": item.requested_id,
                    "node_id": node.get("id") if node is not None else None,
                    "database_id": node.get("databaseId") if node is not None else None,
                    "payload": json.dumps(node, default=str) if node is not None else None,
                    "source_changed_at": format_cursor_ts(changed_at) if changed_at else None,
                    "extracted_at": extracted_at,
                    "dag_run_id": dag_run_id,
                },
                default=str,
            )
        )
    return gzip.compress("\n".join(lines).encode("utf-8"))


def create_landing_table(connection, catalog: str, schema: str, entity: str) -> None:
    """Create the append-only raw landing table for an entity if it is absent.

    Column order matches copy_into_landing's SELECT list: COPY INTO's subquery
    result aligns to the target table positionally, not by name.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("schema", schema)
    table = landing_table(catalog, schema, entity)
    cursor = connection.cursor()
    try:
        execute_with_retry(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        execute_with_retry(
            cursor,
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "  requested_id BIGINT,"
            "  node_id STRING,"
            "  database_id BIGINT,"
            "  payload STRING,"
            "  source_changed_at TIMESTAMP,"
            "  extracted_at TIMESTAMP,"
            "  loaded_at TIMESTAMP,"
            "  dag_run_id STRING"
            ") CLUSTER BY AUTO",
        )
    finally:
        cursor.close()


def copy_into_landing(connection, catalog: str, schema: str, entity: str, s3_uri: str) -> None:
    """Load this run's staged files into the landing table, once, at the end.

    Deliberately not per batch: concurrent fetches finish out of order, so a
    partial load would advance the cursor past ids that never landed and they
    would never be requested again. COPY INTO also tracks the files it has
    already ingested, so a task retry does not double-load.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("schema", schema)
    table = landing_table(catalog, schema, entity)
    cursor = connection.cursor()
    try:
        execute_with_retry(
            cursor,
            f"COPY INTO {table} FROM ("
            "  SELECT requested_id, node_id, database_id, payload, source_changed_at, "
            "         extracted_at, current_timestamp() AS loaded_at, dag_run_id "
            "  FROM :path"
            ") FILEFORMAT = JSON FORMAT_OPTIONS ('compression' = 'gzip')",
            parameters={"path": s3_uri},
        )
    finally:
        cursor.close()


# Four entities share candidacy_worklist_sql: their selections are all inline
# fragments on Candidacy, keyed off the same candidacy id set from the same feed.
ENTITY_SPECS: dict[str, EntitySpec] = {
    "candidacy": EntitySpec("candidacy", "Candidacy", CANDIDACY_SELECTION, 100, candidacy_worklist_sql),
    "party": EntitySpec("party", "Candidacy", PARTY_SELECTION, 100, candidacy_worklist_sql),
    "stance": EntitySpec("stance", "Candidacy", STANCE_SELECTION, 100, candidacy_worklist_sql),
    "endorsement": EntitySpec("endorsement", "Candidacy", ENDORSEMENT_SELECTION, 100, candidacy_worklist_sql),
    "geofence": EntitySpec("geofence", "Geofence", GEOFENCE_SELECTION, 100, geofence_worklist_sql),
    "filing_period": EntitySpec(
        "filing_period", "FilingPeriod", FILING_PERIOD_SELECTION, 100, race_derived_worklist_sql
    ),
    "normalized_position": EntitySpec(
        "normalized_position",
        "NormalizedPosition",
        NORMALIZED_POSITION_SELECTION,
        100,
        partial(position_derived_worklist_sql, field="normalized_position"),
    ),
    "position_election_frequency": EntitySpec(
        "position_election_frequency",
        "PositionElectionFrequency",
        POSITION_ELECTION_FREQUENCY_SELECTION,
        100,
        partial(position_derived_worklist_sql, field="election_frequencies"),
    ),
    "issue": EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, issue_worklist_sql),
}
