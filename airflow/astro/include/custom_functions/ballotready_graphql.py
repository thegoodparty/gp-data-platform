"""Helpers for the ballotready_graphql_person_extract DAG.

Extracts BallotReady (CivicEngine) GraphQL Person objects and lands them raw in
S3. S3 is the durable source of truth; downstream loading (dbt/Airbyte) rebuilds
Databricks from these files.

Person identity is the integer databaseId, which the civics pipeline knows as
``br_candidate_id``. The CivicEngine ``people`` query cannot filter by
updatedAt/createdAt, so persons are fetched by id via ``nodes(ids:)`` and
freshness is driven by the BallotReady S3 feeds (candidacies + office holders),
which carry BR's own created/updated timestamps. A keyset cursor over
``(source_changed_at, br_person_id)`` — persisted in S3 — sweeps the universe on
first run and then only pulls newly-changed persons.
"""

import json
import logging
import random
import re
import time
from base64 import b64encode
from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any

import requests
from databricks.sql.client import Connection

logger = logging.getLogger("airflow.task")

CIVIC_ENGINE_GRAPHQL_URL = "https://bpi.civicengine.com/graphql"

# A Person is addressed by a "Candidate" global id in the CivicEngine graph.
_PERSON_NODE_ID_PREFIX = "gid://ballot-factory/Candidate/"

# Person query mirrors int__ballotready_person.py so landed objects match the
# fields the existing dbt projection relies on.
PERSON_QUERY = """
query GetPersonsBatch($ids: [ID!]!) {
    nodes(ids: $ids) {
        ... on Person {
            bioText
            candidacies(includeUncertified: true) {
                databaseId
                id
            }
            contacts {
                email
                fax
                phone
                type
            }
            createdAt
            databaseId
            degrees {
                databaseId
                degree
                gradYear
                id
                major
                school
            }
            experiences {
                databaseId
                end
                id
                organization
                start
                title
                type
            }
            firstName
            fullName
            id
            images {
                type
                url
            }
            lastName
            middleName
            nickname
            officeHolders {
                nodes {
                    databaseId
                    id
                }
            }
            slug
            suffix
            updatedAt
            urls {
                databaseId
                id
                type
                url
            }
        }
    }
}
"""


def base64_encode_person_id(person_id: int) -> str:
    """Encode an integer BallotReady person id as its base64 GraphQL node id."""
    prefixed = f"{_PERSON_NODE_ID_PREFIX}{person_id}"
    return b64encode(prefixed.encode("utf-8")).decode("utf-8")


def chunked(seq: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive ``size``-length chunks of ``seq``."""
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ---------------------------------------------------------------------------
# HTTP extraction with rate-limit handling
# ---------------------------------------------------------------------------


def _is_retryable_status(status_code: int) -> bool:
    """429 (rate limited) and 5xx (server) responses are worth retrying."""
    return status_code == 429 or status_code >= 500


def _retry_wait_seconds(
    headers: Mapping[str, str],
    attempt: int,
    base_backoff: float,
    max_backoff: float = 60.0,
) -> float:
    """Seconds to wait before the next retry.

    Honors an integer ``Retry-After`` header when present; otherwise exponential
    backoff with full jitter.
    """
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return min(float(retry_after), max_backoff)
        except ValueError:
            pass
    backoff = min(base_backoff * (2**attempt), max_backoff)
    return random.uniform(0, backoff)


def fetch_person_batch(
    person_ids: list[int],
    api_token: str,
    *,
    session: requests.Session | None = None,
    timeout: int = 60,
    max_retries: int = 5,
    base_backoff: float = 1.0,
) -> list[dict[str, Any]]:
    """Fetch a batch of persons by id from the CivicEngine GraphQL API.

    Retries 429/5xx responses with backoff. Returns the non-null Person nodes;
    ids that resolve to a non-Person or are missing come back as nulls and are
    filtered out here.
    """
    http = session or requests
    payload: dict[str, Any] = {
        "query": PERSON_QUERY,
        "variables": {"ids": [base64_encode_person_id(pid) for pid in person_ids]},
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
    }

    for attempt in range(max_retries + 1):
        response = http.post(CIVIC_ENGINE_GRAPHQL_URL, json=payload, headers=headers, timeout=timeout)

        if _is_retryable_status(response.status_code):
            if attempt == max_retries:
                response.raise_for_status()
            wait = _retry_wait_seconds(response.headers, attempt, base_backoff)
            logger.warning(
                "CivicEngine returned %s (attempt %d/%d); retrying in %.1fs",
                response.status_code,
                attempt + 1,
                max_retries,
                wait,
            )
            time.sleep(wait)
            continue

        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(f"CivicEngine GraphQL errors: {body['errors']}")
        nodes = body.get("data", {}).get("nodes", [])
        return [node for node in nodes if node]

    raise RuntimeError("Unreachable: fetch_person_batch exhausted retries without returning")


# ---------------------------------------------------------------------------
# Cursor: which person ids to fetch this run (keyset over the S3 feeds)
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_iso_timestamp(value: str) -> str:
    """Validate and canonicalize the cursor timestamp before inlining it into SQL.

    Preserves sub-second precision so the keyset cursor's tiebreak stays exact
    when many rows share the same second.
    """
    try:
        return datetime.fromisoformat(value).isoformat(sep=" ", timespec="microseconds")
    except ValueError:
        raise ValueError(f"cursor timestamp is not a plain ISO timestamp: {value!r}") from None


def build_person_cursor_query(
    catalog: str,
    dbt_schema: str,
    *,
    after_changed_at: str | None,
    after_person_id: int | None,
    limit: int | None,
) -> str:
    """SQL for the next batch of person ids to fetch, ordered by the keyset cursor.

    ``source_changed_at`` is the greatest created/updated timestamp across a
    person's candidacy and office-holder rows. The keyset predicate
    ``(changed_at, br_person_id) > (cursor_ts, cursor_id)`` advances safely even
    when many rows share an identical bulk-update timestamp.
    """
    for name, val in (("catalog", catalog), ("dbt_schema", dbt_schema)):
        if not _IDENTIFIER_RE.match(val):
            raise ValueError(f"{name} is not a valid SQL identifier: {val!r}")
    candidacies = f"`{catalog}`.`{dbt_schema}`.`stg_airbyte_source__ballotready_s3_candidacies_v3`"
    office_holders = f"`{catalog}`.`{dbt_schema}`.`stg_airbyte_source__ballotready_s3_office_holders_v3`"

    predicate = "source_changed_at IS NOT NULL"
    if after_changed_at is not None and after_person_id is not None:
        ts = _validate_iso_timestamp(after_changed_at)
        pid = int(after_person_id)
        predicate += (
            f" AND (source_changed_at > TIMESTAMP '{ts}' "
            f"OR (source_changed_at = TIMESTAMP '{ts}' AND br_person_id > {pid}))"
        )

    limit_clause = f" LIMIT {int(limit)}" if limit and limit > 0 else ""

    return (
        "WITH source_rows AS ("
        f"  SELECT br_candidate_id AS br_person_id, "
        f"         greatest(candidacy_created_at, candidacy_updated_at) AS changed_at "
        f"  FROM {candidacies} WHERE br_candidate_id IS NOT NULL "
        "  UNION ALL "
        f"  SELECT br_candidate_id AS br_person_id, "
        f"         greatest(office_holder_created_at, office_holder_updated_at) AS changed_at "
        f"  FROM {office_holders} WHERE br_candidate_id IS NOT NULL"
        "), person_change AS ("
        "  SELECT br_person_id, max(changed_at) AS source_changed_at "
        "  FROM source_rows GROUP BY br_person_id"
        ") "
        f"SELECT br_person_id, source_changed_at FROM person_change WHERE {predicate} "
        f"ORDER BY source_changed_at ASC, br_person_id ASC{limit_clause}"
    )


def get_person_ids_to_fetch(
    connection: Connection,
    catalog: str,
    dbt_schema: str,
    *,
    after_changed_at: str | None = None,
    after_person_id: int | None = None,
    limit: int | None = None,
) -> list[tuple[int, datetime]]:
    """Next batch of ``(br_person_id, source_changed_at)`` past the cursor, ordered."""
    query = build_person_cursor_query(
        catalog,
        dbt_schema,
        after_changed_at=after_changed_at,
        after_person_id=after_person_id,
        limit=limit,
    )
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return [(int(pid), changed_at) for pid, changed_at in cursor.fetchall() if pid is not None]
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# S3: cursor state and raw person archive
# ---------------------------------------------------------------------------


def _s3_hook(aws_conn_id: str):
    """Lazy import so DAG parsing does not require the amazon provider."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook(aws_conn_id=aws_conn_id)


def read_cursor(bucket: str, key: str, aws_conn_id: str) -> tuple[str | None, int | None]:
    """Read the keyset cursor from S3. Returns ``(None, None)`` if absent."""
    hook = _s3_hook(aws_conn_id)
    if not hook.check_for_key(key, bucket_name=bucket):
        logger.info("No cursor at s3://%s/%s — starting from the beginning.", bucket, key)
        return None, None
    state = json.loads(hook.read_key(key, bucket_name=bucket))
    return state.get("source_changed_at"), state.get("br_person_id")


def write_cursor(
    bucket: str,
    key: str,
    aws_conn_id: str,
    *,
    source_changed_at: str,
    br_person_id: int,
    dag_run_id: str,
) -> None:
    """Persist the keyset cursor to S3."""
    hook = _s3_hook(aws_conn_id)
    state = {
        "source_changed_at": source_changed_at,
        "br_person_id": br_person_id,
        "dag_run_id": dag_run_id,
    }
    hook.load_string(json.dumps(state), key=key, bucket_name=bucket, replace=True)


def persons_to_ndjson(persons: list[dict[str, Any]], extracted_at: str, dag_run_id: str) -> str:
    """Serialize persons as newline-delimited JSON for the raw S3 archive."""
    lines = [
        json.dumps({**person, "_extracted_at": extracted_at, "_dag_run_id": dag_run_id}, default=str)
        for person in persons
    ]
    return "\n".join(lines)


def write_persons_ndjson(
    bucket: str,
    key: str,
    aws_conn_id: str,
    persons: list[dict[str, Any]],
    extracted_at: str,
    dag_run_id: str,
) -> int:
    """Write a batch of persons to S3 as NDJSON. Returns the row count written."""
    if not persons:
        return 0
    hook = _s3_hook(aws_conn_id)
    hook.load_string(
        persons_to_ndjson(persons, extracted_at, dag_run_id),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    return len(persons)
