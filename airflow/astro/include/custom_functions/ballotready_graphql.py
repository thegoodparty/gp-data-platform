"""Extraction helpers for the extract_ballotready DAG.

Pulls BallotReady (CivicEngine) GraphQL objects by id and lands the raw node
payloads in Databricks. Every entity is addressed the same way, through
`nodes(ids:)` over base64 global ids, so one client and one registry cover all
of them.
"""

import contextlib
import json
import logging
import random
import re
import threading
import time
from base64 import b64encode
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any

import requests
from include.custom_functions.databricks_utils import execute_with_retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger("airflow.task")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _redact(message: str, secret: str | None) -> str:
    """Replace every occurrence of `secret` in `message` with a placeholder.

    A falsy secret is returned unchanged: `"".replace("", "***")`-style logic would
    otherwise insert the placeholder between every character instead of leaving the
    message alone.
    """
    if not secret:
        return message
    return message.replace(secret, "***")


CIVIC_ENGINE_GRAPHQL_URL = "https://bpi.civicengine.com/graphql"
_NODE_ID_PREFIX = "gid://ballot-factory"

# Ids are fetched and inserted one window at a time, in cursor order, so a crash mid-run
# leaves a contiguous prefix committed rather than an out-of-order gap. See extract_entity.
WINDOW_SIZE = 2000

# Secondary bound on rows/statement, kept so a very large number of tiny rows (e.g. a
# nulled-out miss window) cannot separately trip some other per-statement limit. The
# real constraint that forced batching here is MAX_INSERT_PARAM_CHARS, below.
INSERT_BATCH_SIZE = 200

# The server's actual limit is on combined bound-parameter character count, not parameter
# count: Databricks rejects a statement whose parameters exceed 1,048,576 characters
# combined. This budget sits well under that because our estimate only sums the string
# form of each bound value, while payload sizes vary within a batch and the server's own
# accounting may differ slightly from ours.
MAX_INSERT_PARAM_CHARS = 800_000


def encode_node_id(node_type: str, node_id: int) -> str:
    """Encode an integer BallotReady id as its base64 GraphQL global id."""
    return b64encode(f"{_NODE_ID_PREFIX}/{node_type}/{node_id}".encode()).decode("utf-8")


def chunked(seq: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive `size`-length chunks of `seq`."""
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _row_param_chars(row: tuple[Any, ...]) -> int:
    """Combined character size of a row's bound values, matching the server's limit axis.

    `None` contributes zero: it binds as a null parameter, not the text "None". Every
    column is summed, not just payload, so the estimate stays honest if the column set
    changes and a different one comes to dominate.
    """
    return sum(len(str(value)) for value in row if value is not None)


def chunk_rows_for_insert(
    rows: list[tuple[Any, ...]],
    entity: str,
    max_chars: int = MAX_INSERT_PARAM_CHARS,
    max_rows: int = INSERT_BATCH_SIZE,
) -> Iterator[list[tuple[Any, ...]]]:
    """Split rows into statement-sized batches bounded by bound-parameter characters first,
    row count second, preserving row order both within and across batches.

    A single row whose own size exceeds `max_chars` cannot be split further: it is still
    emitted, alone, with a warning naming the entity and its size. The alternative is an
    infinite loop trying to fit it under a budget it cannot meet, or silently dropping it;
    this way it fails loudly at the server instead.
    """
    chunk: list[tuple[Any, ...]] = []
    chunk_chars = 0
    for row in rows:
        row_chars = _row_param_chars(row)
        if row_chars > max_chars:
            if chunk:
                yield chunk
                chunk, chunk_chars = [], 0
            logger.warning(
                "%s: row's bound parameters (%d chars) exceed the %d-char insert budget on "
                "their own; sending it as its own statement",
                entity,
                row_chars,
                max_chars,
            )
            yield [row]
            continue
        if chunk and (chunk_chars + row_chars > max_chars or len(chunk) >= max_rows):
            yield chunk
            chunk, chunk_chars = [], 0
        chunk.append(row)
        chunk_chars += row_chars
    if chunk:
        yield chunk


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

    `api_token` is stripped here regardless of what the caller already did, so a
    stray newline or space from an Airflow Variable can never reach the header.
    """
    api_token = api_token.strip()
    if not api_token:
        raise ValueError("civicengine_api_token is empty or missing")

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
        try:
            response = session.post(CIVIC_ENGINE_GRAPHQL_URL, json=payload, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                # Re-raising `exc` itself (or chaining it with `from exc`) would let Airflow's
                # own failure handler print its raw str() into the task log, which can carry
                # the token (requests embeds the offending header value in exceptions like
                # InvalidHeader). `from None` suppresses the chain so that traceback is never
                # printed; this loses the original frames, accepted over leaking a token into
                # logs that persist in S3.
                raise RuntimeError(
                    f"CivicEngine request failed for {len(ids)} {node_type} ids after "
                    f"{max_retries} retries: {type(exc).__name__}: {_redact(str(exc), api_token)}"
                ) from None
            wait = retry_wait_seconds({}, attempt)
            # requests embeds the offending header value in exceptions like InvalidHeader, so
            # the exception's own text could carry the bearer token; redact before logging it.
            logger.warning(
                "CivicEngine request failed for %d %s ids (attempt %d/%d); retrying in %.1fs: %s: %s",
                len(ids),
                node_type,
                attempt + 1,
                max_retries,
                wait,
                type(exc).__name__,
                _redact(str(exc), api_token),
            )
            sleep(wait)
            continue

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
    # Other entities whose landing tables this entity's worklist reads (e.g. issue reads
    # stance's). extract_entity must create these too, or an entities-filtered run that
    # skips the other side never creates the table this one's worklist queries.
    reads_tables: tuple[str, ...] = ()


def landing_table(catalog: str, schema: str, entity: str) -> str:
    """Fully qualified, backtick-quoted landing table for an entity."""
    return f"`{catalog}`.`{schema}`.`ballotready_{entity}_raw`"


def validate_identifier(name: str, value: str) -> str:
    """Identifiers cannot be bound as parameters in DDL, so they are validated instead."""
    if not _IDENTIFIER_RE.match(value or ""):
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def format_cursor_ts(value: datetime | str) -> str:
    """Render a Databricks timestamp as a tz-naive UTC literal, keeping sub-second precision.

    Precision matters: the tiebreak is only exact if ties really are ties. A `str` is
    accepted too, and parsed as ISO-8601, so a staging column that regresses to STRING
    (as happened once already) fails with a clear parse error here rather than an
    AttributeError deep inside build_insert_rows.
    """
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"format_cursor_ts: not a valid ISO-8601 timestamp: {value!r}") from exc
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
    dbt_schema: str,
    *,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Candidacy ids from the S3 feed plus the upcoming-race roster.

    The S3 feed omits many upcoming general-stage rosters that the API race
    object already carries, and without them those candidacies are never
    fetched. The upcoming-roster query is inlined from what used to be a dbt
    intermediate model (an explode of two staging models), so this reads only
    staging rather than reaching into dbt's intermediate layer.
    """
    validate_identifier("catalog", catalog)
    validate_identifier("dbt_schema", dbt_schema)
    base = f"`{catalog}`.`{dbt_schema}`"
    upcoming = (
        "SELECT cast(candidacy.databaseId AS bigint) AS br_candidacy_id, "
        "max(r.updated_at) AS race_updated_at "
        f"FROM {base}.`stg_airbyte_source__ballotready_api_race` r "
        "LATERAL VIEW explode(r.candidacies) AS candidacy "
        "WHERE r.election.databaseId IN (SELECT database_id "
        f"FROM {base}.`stg_airbyte_source__ballotready_api_election` "
        "WHERE election_day >= current_date()) "
        "AND candidacy.databaseId IS NOT NULL "
        "GROUP BY cast(candidacy.databaseId AS bigint)"
    )
    inner = (
        "SELECT cast(br_candidacy_id AS bigint) AS source_id, "
        # candidacy_updated_at is STRING in this staging model (candidacy_created_at is
        # already cast there); cast the greatest() result so both UNION branches agree on type.
        "cast(greatest(candidacy_created_at, candidacy_updated_at) AS timestamp) AS source_changed_at "
        f"FROM {base}.`stg_airbyte_source__ballotready_s3_candidacies_v3` "
        "WHERE br_candidacy_id IS NOT NULL "
        "UNION ALL "
        "SELECT br_candidacy_id AS source_id, race_updated_at AS source_changed_at "
        f"FROM ({upcoming}) upcoming"
    )
    grouped = (
        f"SELECT source_id, max(source_changed_at) AS source_changed_at FROM ({inner}) GROUP BY source_id"
    )
    return _worklist(grouped, _keyset_predicate(after_changed_at, after_source_id), limit)


def geofence_worklist_sql(
    catalog: str,
    dbt_schema: str,
    *,
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
    validate_identifier("dbt_schema", dbt_schema)
    table = f"`{catalog}`.`{dbt_schema}`.`stg_airbyte_source__ballotready_s3_candidacies_v3`"
    inner = (
        "SELECT cast(br_geofence_id AS bigint) AS source_id, "
        # candidacy_updated_at is STRING in this staging model; cast it so source_changed_at
        # is always a real timestamp, matching every other worklist builder.
        "cast(candidacy_updated_at AS timestamp) AS source_changed_at "
        f"FROM {table} WHERE br_geofence_id IS NOT NULL"
    )
    grouped = (
        f"SELECT source_id, max(source_changed_at) AS source_changed_at FROM ({inner}) GROUP BY source_id"
    )
    return _worklist(grouped, _keyset_predicate(after_changed_at, after_source_id), limit)


def race_derived_worklist_sql(
    catalog: str,
    dbt_schema: str,
    *,
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
    validate_identifier("dbt_schema", dbt_schema)
    table = f"`{catalog}`.`{dbt_schema}`.`stg_airbyte_source__ballotready_api_race`"
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
    dbt_schema: str,
    *,
    field: str,
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
    validate_identifier("dbt_schema", dbt_schema)
    table = f"`{catalog}`.`{dbt_schema}`.`stg_airbyte_source__ballotready_api_position`"
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
    dbt_schema: str,
    *,
    source_schema: str | None = None,
    after_changed_at: str | None = None,
    after_source_id: int | None = None,
    limit: int,
) -> str:
    """Issue ids referenced by landed stances that have not been fetched yet.

    Issues have no timestamped feed to key a cursor on, and the set is small and
    slow-changing, so "everything referenced but not yet landed" is both correct
    and cheap. dbt_schema and the keyset cursor args are accepted but unused, so
    this builder is callable identically to the rest. Because of this,
    `full_reload` has no effect on issue: there is no cursor to ignore, and the
    anti-join against already-landed rows always applies.
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


def build_insert_rows(
    fetched: list[FetchedNode],
    changed_at_by_id: Mapping[int, datetime | str],
    extracted_at: str,
    dag_run_id: str,
) -> list[tuple[int, str | None, int | None, str | None, str | None, str, str]]:
    """One row per requested id, in landing table column order minus loaded_at.

    A row is built for every requested id, with a null payload where the API
    returned nothing. Skipping those would leave the id below the cursor forever
    and would read downstream as a deletion rather than an absence. `node is not
    None` (not truthiness) decides a hit, because an empty dict is a real payload.
    """
    rows = []
    for item in fetched:
        node = item.node
        changed_at = changed_at_by_id.get(item.requested_id)
        rows.append(
            (
                item.requested_id,
                node.get("id") if node is not None else None,
                node.get("databaseId") if node is not None else None,
                json.dumps(node, default=str) if node is not None else None,
                format_cursor_ts(changed_at) if changed_at is not None else None,
                extracted_at,
                dag_run_id,
            )
        )
    return rows


def create_landing_table(connection, catalog: str, schema: str, entity: str) -> None:
    """Create the append-only raw landing table for an entity if it is absent.

    Column order matches insert_rows's column list.
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


def insert_rows(
    connection,
    catalog: str,
    schema: str,
    entity: str,
    rows: list[tuple[int, str | None, int | None, str | None, str | None, str, str]],
) -> None:
    """Append rows to the landing table in one or more parameterized multi-row INSERTs.

    Chunked by bound-parameter character size (see MAX_INSERT_PARAM_CHARS), with
    INSERT_BATCH_SIZE as a secondary cap on rows per statement. Always an INSERT, never
    a MERGE: duplicates are expected on a full_reload or a genuine BallotReady change,
    and downstream dedup on (requested_id, max(loaded_at)) resolves them.
    """
    if not rows:
        return
    validate_identifier("catalog", catalog)
    validate_identifier("schema", schema)
    table = landing_table(catalog, schema, entity)
    cursor = connection.cursor()
    try:
        for chunk in chunk_rows_for_insert(rows, entity):
            value_groups = []
            parameters: dict[str, Any] = {}
            for i, row in enumerate(chunk):
                requested_id, node_id, database_id, payload, source_changed_at, extracted_at, dag_run_id = row
                value_groups.append(
                    f"(:requested_id_{i}, :node_id_{i}, :database_id_{i}, :payload_{i}, "
                    f"cast(:source_changed_at_{i} AS TIMESTAMP), cast(:extracted_at_{i} AS TIMESTAMP), "
                    f"current_timestamp(), :dag_run_id_{i})"
                )
                parameters[f"requested_id_{i}"] = requested_id
                parameters[f"node_id_{i}"] = node_id
                parameters[f"database_id_{i}"] = database_id
                parameters[f"payload_{i}"] = payload
                parameters[f"source_changed_at_{i}"] = source_changed_at
                parameters[f"extracted_at_{i}"] = extracted_at
                parameters[f"dag_run_id_{i}"] = dag_run_id
            sql = (
                f"INSERT INTO {table} (requested_id, node_id, database_id, payload, "
                "source_changed_at, extracted_at, loaded_at, dag_run_id) VALUES " + ", ".join(value_groups)
            )
            execute_with_retry(cursor, sql, parameters=parameters)
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
    "issue": EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, issue_worklist_sql, reads_tables=("stance",)),
}


@dataclass(frozen=True)
class ExtractConfig:
    """Per-run parameters threaded through every entity's extract task.

    dbt_schema is the one dbt read schema a worklist query may need (the
    stg_airbyte_source__ballotready_* models). source_schema is where this
    DAG's own landing tables live (it is what issue_worklist_sql reads landed
    stance/issue rows back out of). dag_run_id is stamped into the landed rows.
    """

    catalog: str
    dbt_schema: str
    source_schema: str
    api_token: str
    max_ids: int
    max_workers: int
    requests_per_second: float
    full_reload: bool
    dag_run_id: str
    extracted_at: str


def make_session(max_workers: int) -> requests.Session:
    """A session whose connection pool can actually serve `max_workers` requests at once.

    requests.Session's default pool holds 10 connections; below max_workers,
    extra threads queue on the pool instead of the network and the
    concurrency configured by max_workers never materializes.
    """
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def read_worklist(
    connection,
    spec: EntitySpec,
    config: ExtractConfig,
    after: tuple[datetime | None, int | None],
) -> list[tuple[int, datetime]]:
    """Run one entity's worklist query for at most config.max_ids ids.

    Every builder in ENTITY_SPECS takes the same keyword signature, so this
    never branches on entity name.
    """
    after_changed_at, after_source_id = after
    sql = spec.worklist_sql(
        config.catalog,
        config.dbt_schema,
        source_schema=config.source_schema,
        after_changed_at=format_cursor_ts(after_changed_at) if after_changed_at is not None else None,
        after_source_id=after_source_id,
        limit=config.max_ids,
    )
    cursor = connection.cursor()
    try:
        execute_with_retry(cursor, sql)
        return [(int(row[0]), row[1]) for row in cursor.fetchall()]
    finally:
        cursor.close()


def extract_entity(spec: EntitySpec, connection, config: ExtractConfig) -> dict:
    """Extract one entity end to end: cursor, worklist, concurrent fetch, land.

    The worklist arrives sorted by (source_changed_at, source_id) ascending. It is
    fetched and inserted one WINDOW_SIZE-sized window at a time, in that order, rather
    than fetched wholesale and loaded once. The cursor is derived from the landing
    table's own max (source_changed_at, requested_id); an out-of-order insert (e.g. one
    committing whichever batch happens to finish first under threading) could push that
    max ahead of an id that never landed, and that id would then sit below the cursor
    forever. Landing windows in cursor order instead means a crash mid-run always leaves
    a contiguous prefix committed, so the cursor read on retry is exactly right and
    picks back up where the failure left off.
    """
    create_landing_table(connection, config.catalog, config.source_schema, spec.name)
    # An entities-filtered run can skip the task that would normally create this table
    # first; create it here too so this entity's worklist never queries a table that
    # was never made.
    for other in spec.reads_tables:
        create_landing_table(connection, config.catalog, config.source_schema, other)

    after: tuple[datetime | None, int | None] = (
        (None, None)
        if config.full_reload
        else read_cursor(connection, config.catalog, config.source_schema, spec.name)
    )

    worklist = read_worklist(connection, spec, config, after)
    if not worklist:
        return {
            "entity": spec.name,
            "ids_requested": 0,
            "rows_written": 0,
            "windows": 0,
            "cursor_source_changed_at": format_cursor_ts(after[0]) if after[0] is not None else None,
            "cursor_requested_id": after[1],
        }

    changed_at_by_id = dict(worklist)
    ids = [source_id for source_id, _ in worklist]

    rows_written = 0
    windows = 0
    with contextlib.closing(make_session(config.max_workers)) as session:
        limiter = RateLimiter(config.requests_per_second)

        def fetch_batch(batch: list[int]) -> list[FetchedNode]:
            return fetch_nodes(batch, spec.node_type, spec.selection, config.api_token, limiter, session)

        # Each window's batches fan out across the pool concurrently (fine: order
        # within a window is fixed up by the sort below), but windows themselves run
        # one at a time and each is fully inserted before the next one starts. Do not
        # "optimize" this into one pool across every window -- that reintroduces the
        # out-of-order landing this loop exists to prevent (see the docstring above).
        for window_ids in chunked(ids, WINDOW_SIZE):
            batches = list(chunked(window_ids, spec.batch_size))
            fetched: list[FetchedNode] = []
            with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
                futures = [executor.submit(fetch_batch, batch) for batch in batches]
                try:
                    for future in as_completed(futures):
                        fetched.extend(future.result())  # raises before this window's INSERT
                except Exception:
                    # One failed batch must not let every already-submitted batch in
                    # this window keep hitting the live API before this surfaces.
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise

            rows = build_insert_rows(fetched, changed_at_by_id, config.extracted_at, config.dag_run_id)
            rows.sort(key=lambda row: (row[4], row[0]))
            insert_rows(connection, config.catalog, config.source_schema, spec.name, rows)
            rows_written += len(rows)
            windows += 1

    last_id, last_changed_at = worklist[-1]
    return {
        "entity": spec.name,
        "ids_requested": len(worklist),
        "rows_written": rows_written,
        "windows": windows,
        # Formatted so the UI summary matches the cursor format used everywhere else.
        "cursor_source_changed_at": format_cursor_ts(last_changed_at)
        if last_changed_at is not None
        else None,
        "cursor_requested_id": last_id,
    }
