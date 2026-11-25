"""HubSpot API client and landing helpers for the companion loader.

The companion loader lands the entities Lakeflow Connect cannot produce. Today this
module carries one of them: activity-to-contact associations for calls, meetings, notes
and tasks. The Lakeflow activity tables carry no association column and no ARRAY column
of any kind, and the single-id fallbacks reach 2.3% on calls and 25.0% on tasks, so
`int__user_hubspot_touches` would drop a Lakeflow-owned, unassociated activity from the
touch history entirely rather than lose one column.

Two properties of the landing grain are deliberate:

- **One complete snapshot per activity**, carrying every linked contact id, with an
  explicit empty array when it has none. A link-per-row append cannot represent the
  removal of an activity's last link: the removed contact would survive forever as its
  latest recorded link.
- **Incremental on the activity's own `hs_lastmodifieddate`**, because a full sweep per
  run is not affordable (calls alone is roughly 700k rows).

**The cursor does not see a pure association change.** Adding or removing a contact link
does not move an activity's `hs_lastmodifieddate`, and the unseen branch only recovers ids
with no landing row at all, so a link removed on an already-landed activity is invisible
to an incremental run. The cadence therefore has to include a periodic `full_sweep=True`
run, which is the only thing that captures a removal. That is not a simplification to
tidy away later: the cost is one full pass (under a thousand API calls for every type),
and the alternative is that a stale link survives forever as the latest snapshot.

Only **contact** associations are read. Nothing reads a company or deal association on an
activity, so landing them would be rows nothing consumes; the same endpoint extends to
both (`/crm/v4/associations/<activity>/companies/batch/read` and `.../deals/batch/read`)
if a future model needs them.

Requests are sequential, one per 1,000 activity ids, so a full first sweep of every
activity type is under a thousand calls. A 429 pauses and retries; there is no proactive
rate limiter, because the batch width already keeps the call count two orders of
magnitude below where HubSpot's per-second limit binds.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import batched
from typing import Any, NamedTuple

import requests
from include.custom_functions.databricks_utils import execute_with_retry

logger = logging.getLogger("airflow.task")

HUBSPOT_API_BASE_URL = "https://api.hubapi.com"

# HubSpot's documented ceiling on ids per v4 association batch read. Above it the request
# is rejected rather than truncated, so this is a hard boundary and not a preference.
ASSOCIATION_BATCH_SIZE = 1000

# Bound on rows per INSERT. The real server limit is the combined CHARACTER size of bound
# parameters (1,048,576), not their count, and a contact-ids array can be large on its
# own, so both are enforced (see chunk_rows_for_insert).
INSERT_BATCH_SIZE = 200
MAX_INSERT_PARAM_CHARS = 800_000

# Drain size for the worklist cursor. Bounds how many connector row objects are alive at
# once, not how many ids are fetched per API pass.
WORKLIST_FETCH_CHUNK = 10_000

ACTIVITY_ASSOCIATION_TABLE = "companion_activity_contact_associations"

# The activity types with a Lakeflow table of the same name and a v4 association endpoint.
# Email is absent on purpose: its Lakeflow row already carries a single contact id, which
# reaches 99.6% of rows, so it does not need this loader to cut over.
ACTIVITY_OBJECTS: tuple[tuple[str, str], ...] = (
    ("calls", "CALL"),
    ("meetings", "MEETING"),
    ("notes", "NOTE"),
    ("tasks", "TASK"),
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_ACTIVITY_TYPE_RE = re.compile(r"^[A-Z][A-Z_]*$")
_ACTIVITY_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _redact(message: str, secret: str | None) -> str:
    """Replace every occurrence of `secret` in `message` with a placeholder.

    A falsy secret is returned unchanged: replacing an empty string would otherwise
    insert the placeholder between every character.
    """
    if not secret:
        return message
    return message.replace(secret, "***")


class LandedAssociation(NamedTuple):
    """One landing row, minus loaded_at, which the INSERT stamps server-side."""

    activity_type: str
    activity_id: str
    contact_ids_json: str
    source_changed_at: str
    extracted_at: str
    dag_run_id: str


# The landing table's columns in table order. The DDL, the INSERT column list and the
# bound-parameter names all derive from this, so a column added here reaches every
# statement without a second edit.
LANDING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("activity_type", "STRING"),
    ("activity_id", "STRING"),
    ("contact_ids_json", "STRING"),
    ("source_changed_at", "TIMESTAMP"),
    ("extracted_at", "TIMESTAMP"),
    ("loaded_at", "TIMESTAMP"),
    ("dag_run_id", "STRING"),
)

# Set server-side rather than bound, so no LandedAssociation field backs it.
_SERVER_SET_COLUMNS = {"loaded_at": "current_timestamp()"}

# Bound as strings, so the statement has to read them back as timestamps.
_TIMESTAMP_COLUMNS = frozenset({"source_changed_at", "extracted_at"})

INSERT_COLUMNS: tuple[str, ...] = tuple(
    name for name, _ in LANDING_COLUMNS if name not in _SERVER_SET_COLUMNS
)

# A field that stopped lining up with its column would bind every later value one position
# off, which no test of either alone would catch. Raised rather than asserted so the check
# survives python -O.
if LandedAssociation._fields != INSERT_COLUMNS:
    raise RuntimeError(
        f"LandedAssociation fields {LandedAssociation._fields} must match the landing "
        f"columns {INSERT_COLUMNS}, in order"
    )


def validate_identifier(name: str, value: str) -> str:
    """Identifiers cannot be bound as parameters in DDL, so they are validated instead."""
    if not _IDENTIFIER_RE.match(value or ""):
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def split_qualified_name(value: str, variable_name: str) -> tuple[str, str]:
    """Split a `catalog.schema` Airflow Variable into its two validated parts.

    Both the worklist's source relation and the landing schema are Variables, so the D3
    cutover, which moves the activity tables into `raw.hubspot` where the loader's principal
    has no SELECT, is a configuration change rather than a code change.

    `variable_name` is in the error because a malformed Variable is fixed in the Airflow UI,
    and the message has to name which one.
    """
    parts = (value or "").strip().split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"{variable_name} must be `catalog.schema`, got {value!r}")
    return (
        validate_identifier(f"{variable_name} catalog", parts[0]),
        validate_identifier(f"{variable_name} schema", parts[1]),
    )


def validate_source_prefix(prefix: str) -> tuple[str, str]:
    """The worklist's `catalog.schema`, validated."""
    return split_qualified_name(prefix, "hubspot_companion_activity_source")


def format_cursor_ts(value: datetime | str) -> str:
    """Render a Databricks timestamp as a tz-naive UTC literal, keeping sub-second precision."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"format_cursor_ts: not a valid ISO-8601 timestamp: {value!r}") from exc
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.isoformat(sep=" ", timespec="microseconds")


def landing_relation(catalog: str, schema: str) -> str:
    """Fully qualified, backtick-quoted landing table."""
    validate_identifier("catalog", catalog)
    validate_identifier("schema", schema)
    return f"`{catalog}`.`{schema}`.`{ACTIVITY_ASSOCIATION_TABLE}`"


def is_retryable_status(status_code: int) -> bool:
    """429 (rate limited) and 5xx (server) responses are worth retrying."""
    return status_code == 429 or status_code >= 500


def retry_wait_seconds(
    headers: Mapping[str, str],
    attempt: int,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
) -> float:
    """Seconds to wait before the next retry: Retry-After when present, else backoff."""
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return max(0.0, min(float(retry_after), max_backoff))
        except ValueError:
            pass
    return min(base_backoff * (2**attempt), max_backoff)


def _post(
    session: requests.Session,
    path: str,
    body: Mapping[str, Any],
    api_token: str,
    describe: str,
    max_retries: int = 5,
    timeout: int = 60,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """POST one HubSpot request and return its decoded body, retrying what is retryable.

    `api_token` is stripped here regardless of what the caller did, so a stray newline or
    space pasted into an Airflow Variable can never reach the header. Errors are raised
    with the token redacted: requests embeds the offending header value in some of its own
    exceptions, and Airflow persists task logs.
    """
    api_token = api_token.strip()
    if not api_token:
        raise ValueError("hubspot_companion_loader_token is empty or missing")

    url = HUBSPOT_API_BASE_URL + path
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {api_token}",
    }

    for attempt in range(max_retries + 1):
        try:
            response = session.post(url, json=body, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries:
                # `from None` suppresses the chained traceback, which can carry the header
                # value; the message alone is enough to act on.
                raise RuntimeError(
                    f"HubSpot request failed for {describe} after {max_retries} retries: "
                    f"{type(exc).__name__}: {_redact(str(exc), api_token)}"
                ) from None
            wait = retry_wait_seconds({}, attempt)
            logger.warning(
                "HubSpot request failed for %s (attempt %d/%d); retrying in %.1fs: %s: %s",
                describe,
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
                raise RuntimeError(
                    f"HubSpot returned {response.status_code} for {describe} after "
                    f"{max_retries} retries: {_redact(response.text[:300], api_token)}"
                )
            wait = retry_wait_seconds(response.headers, attempt)
            logger.warning(
                "HubSpot returned %s for %s (attempt %d/%d); retrying in %.1fs",
                response.status_code,
                describe,
                attempt + 1,
                max_retries,
                wait,
            )
            sleep(wait)
            continue

        if response.status_code not in (200, 207):
            raise RuntimeError(
                f"HubSpot returned {response.status_code} for {describe}: "
                f"{_redact(response.text[:300], api_token)}"
            )
        return response.json()

    raise RuntimeError("Unreachable: _post exhausted retries without returning")


# Errors that mean "this object has no such associations" rather than "the request
# failed". They are landed as an explicit empty array, which is the whole point of the
# snapshot grain: a 207 carrying one of these for an unlinked activity is success.
NO_ASSOCIATIONS_SUBCATEGORIES = frozenset(
    {
        "crm.associations.NO_ASSOCIATIONS_FOUND",
        "crm.associations.ASSOCIATION_NOT_FOUND",
    }
)


def _is_no_associations_error(error: Mapping[str, Any]) -> bool:
    """Whether a batch error is HubSpot saying the object has no links.

    The subCategory can sit on the entry or on any of its nested `errors`, so both are
    read. Anything else is a real failure and must not land as an empty snapshot.
    """
    candidates = [error.get("subCategory")]
    candidates += [inner.get("subCategory") for inner in error.get("errors") or []]
    return any(isinstance(value, str) and value in NO_ASSOCIATIONS_SUBCATEGORIES for value in candidates)


def parse_association_batch(body: Mapping[str, Any], requested_ids: Sequence[str]) -> dict[str, list[str]]:
    """Map every requested activity id to its distinct contact ids, `[]` when it has none.

    HubSpot omits an object with no associations from `results` (and some responses carry
    an entry with an empty `to`), so a missing entry and an empty one must both produce an
    explicit empty array rather than no row. `results` is keyed by `from.id` rather than by
    position, because the v4 batch endpoint does not promise to answer in request order.

    A `207` carries `results` and `errors[]` together, and an activity with no links can
    appear in either. So a no-associations error is tolerated (its id keeps the empty array
    the map already holds) and any other error raises: an errored id landed as "no links"
    would be indistinguishable downstream from a real removal.
    """
    for error in body.get("errors") or []:
        if _is_no_associations_error(error):
            continue
        raise RuntimeError(f"HubSpot association batch reported errors: {error}"[:500])

    by_id: dict[str, set[str]] = {activity_id: set() for activity_id in requested_ids}
    for result in body.get("results") or []:
        from_id = str((result.get("from") or {}).get("id") or "").strip()
        if not from_id:
            raise RuntimeError(f"HubSpot association result has no from.id: {result!r}"[:300])
        if from_id not in by_id:
            # Merging it would attribute links to an id this batch never asked about, and a
            # shifted response would land every later row on the wrong activity.
            raise RuntimeError(
                f"HubSpot association result is for activity {from_id}, which this batch did not request"
            )
        for link in result.get("to") or []:
            to_object_id = str(link.get("toObjectId") or "").strip()
            if to_object_id:
                # Every toObjectId whatever its association type: filtering on typeId would
                # drop a link carrying only the unlabeled type.
                by_id[from_id].add(to_object_id)
    return {activity_id: sorted(contact_ids) for activity_id, contact_ids in by_id.items()}


def _paged_ids(body: Mapping[str, Any]) -> dict[str, str]:
    """Activity ids whose association list is truncated, mapped to their continuation token.

    A result carries `paging.next.after` when it has more links than one response holds,
    and the docs say that token is fed back alongside the same id. Ignoring it would land a
    partial snapshot for a heavily linked activity and look like a real shortfall in the
    pairs check.
    """
    more: dict[str, str] = {}
    for result in body.get("results") or []:
        after = ((result.get("paging") or {}).get("next") or {}).get("after")
        from_id = str((result.get("from") or {}).get("id") or "").strip()
        if after and from_id:
            more[from_id] = str(after)
    return more


def fetch_contact_associations(
    session: requests.Session,
    activity_object: str,
    activity_ids: Sequence[str],
    api_token: str,
    batch_size: int = ASSOCIATION_BATCH_SIZE,
    max_pages: int = 20,
) -> dict[str, list[str]]:
    """Read activity-to-contact links for `activity_ids`, one v4 batch call per 1,000 ids.

    Returns one entry per requested id, in every case, so the caller can land a complete
    snapshot without consulting the response again. An id whose links do not fit one
    response is re-requested with its continuation token until it stops paging, so the
    snapshot really is the complete set.

    `max_pages` bounds that loop. A server that kept returning a continuation token would
    otherwise loop forever, and the failure it should produce is a loud one.
    """
    validate_identifier("activity object", activity_object)
    if not activity_ids:
        return {}
    if batch_size < 1 or batch_size > ASSOCIATION_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {ASSOCIATION_BATCH_SIZE}")

    def read(batch: Sequence[str], after_by_id: Mapping[str, str]) -> dict[str, list[str]]:
        inputs = []
        for activity_id in batch:
            entry: dict[str, Any] = {"id": activity_id}
            if activity_id in after_by_id:
                entry["after"] = after_by_id[activity_id]
            inputs.append(entry)
        body = _post(
            session,
            f"/crm/v4/associations/{activity_object}/contacts/batch/read",
            {"inputs": inputs},
            api_token,
            f"{len(batch)} {activity_object} association ids",
        )
        return body

    associations: dict[str, list[str]] = {}
    # strict=False: the final batch is expected to be short.
    for batch in batched(activity_ids, batch_size, strict=False):
        merged: dict[str, set[str]] = {}
        # Only the ids still paging are re-requested. Sending the whole batch again would
        # re-fetch page one for every id that had already finished, which restarts it: two
        # ids needing 7 and 11 pages would then only both finish after 77 requests and never
        # within the bound. It also wastes a full batch of reads per page.
        to_read: Sequence[str] = list(batch)
        pending: dict[str, str] = {}
        for _ in range(max_pages):
            body = read(to_read, pending)
            page_links = parse_association_batch(body, to_read)
            for activity_id, contact_ids in page_links.items():
                merged.setdefault(activity_id, set()).update(contact_ids)
            pending = _paged_ids(body)
            if not pending:
                break
            to_read = [activity_id for activity_id in to_read if activity_id in pending]
        else:
            raise RuntimeError(
                f"HubSpot kept returning a continuation token for {len(pending)} "
                f"{activity_object} ids after {max_pages} pages"
            )
        for activity_id in batch:
            associations[activity_id] = sorted(merged.get(activity_id, set()))
    return associations


def build_snapshot_rows(
    associations: Mapping[str, Sequence[str]],
    changed_at_by_id: Mapping[str, datetime | str],
    activity_type: str,
    extracted_at: str,
    dag_run_id: str,
) -> list[LandedAssociation]:
    """One snapshot row per activity, ordered by (source_changed_at, activity_id).

    The order is what makes the derived cursor correct: rows land in worklist order, so a
    run that dies part way leaves a contiguous prefix and the next run resumes from it.
    """
    if not _ACTIVITY_TYPE_RE.match(activity_type or ""):
        raise ValueError(f"not a valid activity type: {activity_type!r}")
    rows = []
    for activity_id, contact_ids in associations.items():
        # Subscript, not .get(): a missing key would land a null source_changed_at, which
        # the cursor's max() then ignores silently. The caller builds this map from the
        # same ids it requested.
        changed_at = changed_at_by_id[activity_id]
        rows.append(
            LandedAssociation(
                activity_type=activity_type,
                activity_id=activity_id,
                contact_ids_json=json.dumps(list(contact_ids), separators=(",", ":")),
                source_changed_at=format_cursor_ts(changed_at),
                extracted_at=extracted_at,
                dag_run_id=dag_run_id,
            )
        )
    rows.sort(key=lambda row: (row.source_changed_at, row.activity_id))
    return rows


def _row_param_chars(row: tuple[Any, ...]) -> int:
    """Combined character size of a row's bound values, matching the server's limit axis."""
    return sum(len(str(value)) for value in row if value is not None)


def chunk_rows_for_insert(
    rows: Sequence[LandedAssociation],
    max_chars: int = MAX_INSERT_PARAM_CHARS,
    max_rows: int = INSERT_BATCH_SIZE,
) -> list[list[LandedAssociation]]:
    """Split rows into statement-sized batches, bounded by parameter characters then rows.

    Same parameter-marker ceiling as the BallotReady loader: it is a property of the
    server, and a wide contact-ids array can exhaust it well before the row count does.
    A single row larger than the budget is sent alone, which fails loudly at the server
    rather than silently dropping the row.
    """
    chunks: list[list[LandedAssociation]] = []
    chunk: list[LandedAssociation] = []
    chunk_chars = 0
    for row in rows:
        row_chars = _row_param_chars(row)
        if chunk and (chunk_chars + row_chars > max_chars or len(chunk) >= max_rows):
            chunks.append(chunk)
            chunk, chunk_chars = [], 0
        chunk.append(row)
        chunk_chars += row_chars
    if chunk:
        chunks.append(chunk)
    return chunks


def _value_group(index: int) -> str:
    """One row's VALUES group, in landing table column order."""
    values = []
    for column, _ in LANDING_COLUMNS:
        if column in _SERVER_SET_COLUMNS:
            values.append(_SERVER_SET_COLUMNS[column])
        elif column in _TIMESTAMP_COLUMNS:
            values.append(f"cast(:{column}_{index} AS TIMESTAMP)")
        else:
            values.append(f":{column}_{index}")
    return "(" + ", ".join(values) + ")"


def create_landing_table(connection, catalog: str, schema: str) -> None:
    """Create the append-only landing table if it is absent.

    The schema itself is never created here. `raw.hubspot` is a production securable that
    its owner creates and hands to the loader's role, and a schema created implicitly by a
    job would belong to whatever identity that job ran as.
    """
    table = landing_relation(catalog, schema)
    columns = ", ".join(f"{name} {column_type}" for name, column_type in LANDING_COLUMNS)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, f"CREATE TABLE IF NOT EXISTS {table} ({columns}) CLUSTER BY AUTO")


def insert_rows(connection, catalog: str, schema: str, rows: Sequence[LandedAssociation]) -> None:
    """Append rows with parameterised multi-row INSERTs.

    Always an INSERT, never a MERGE: a repeated snapshot is expected on a re-read, and
    downstream keeps the latest one per (activity_type, activity_id).
    """
    if not rows:
        return
    table = landing_relation(catalog, schema)
    columns = ", ".join(name for name, _ in LANDING_COLUMNS)
    with contextlib.closing(connection.cursor()) as cursor:
        for chunk in chunk_rows_for_insert(rows):
            value_groups = []
            parameters: dict[str, Any] = {}
            for i, row in enumerate(chunk):
                value_groups.append(_value_group(i))
                parameters.update(
                    {f"{column}_{i}": value for column, value in zip(INSERT_COLUMNS, row, strict=True)}
                )
            sql = f"INSERT INTO {table} ({columns}) VALUES " + ", ".join(value_groups)
            execute_with_retry(cursor, sql, parameters=parameters)


def read_cursor(
    connection, catalog: str, schema: str, activity_type: str
) -> tuple[datetime | None, str | None]:
    """The highest (source_changed_at, activity_id) already landed for this activity type.

    Derived from the landing table rather than kept separately, so a run that dies part way
    leaves a cursor that is exactly true.
    """
    if not _ACTIVITY_TYPE_RE.match(activity_type or ""):
        raise ValueError(f"not a valid activity type: {activity_type!r}")
    table = landing_relation(catalog, schema)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(
            cursor,
            f"SELECT source_changed_at, activity_id FROM {table} "
            f"WHERE activity_type = '{activity_type}' "
            "ORDER BY source_changed_at DESC, activity_id DESC LIMIT 1",
        )
        row = cursor.fetchone()
        return (row[0], str(row[1])) if row else (None, None)


def _cursor_pair(after_changed_at: datetime | None, after_activity_id: str | None) -> tuple[str, str] | None:
    """The cursor as a usable pair, or None when either half is missing.

    One half alone reads as no cursor at all: an over-broad worklist is safe, whereas
    raising here would stall a run over a partial value.
    """
    if after_changed_at is None or after_activity_id is None:
        return None
    if not _ACTIVITY_ID_RE.match(str(after_activity_id)):
        raise ValueError(f"not a valid activity id for a cursor: {after_activity_id!r}")
    return format_cursor_ts(after_changed_at), str(after_activity_id)


def activity_worklist_sql(
    source_prefix: str,
    activity_object: str,
    activity_type: str,
    landing_table: str,
    after_changed_at: datetime | None = None,
    after_activity_id: str | None = None,
) -> str:
    """The activities to read this run: those at or past the cursor, then never-landed ones.

    `source_prefix` is the `catalog.schema` holding the activity tables, supplied from
    configuration because those tables move from the Lakeflow dev schema into `raw.hubspot`
    at the D3 cutover. The landing table is passed in already quoted, since it is a
    different schema from the source.

    Two branches, both needed:

    - the keyset page above the cursor, which is the incremental read; and
    - every activity with no landing row at all, whatever its timestamp, which recovers an
      activity whose timestamp is behind the cursor (a run narrowed by the `activities`
      param, a pipeline backfill that arrives with an old modified date). Those rows sort
      first and never become the cursor.

    Without a cursor the second branch is omitted: a full sweep already lists everything.
    """
    catalog, schema = validate_source_prefix(source_prefix)
    validate_identifier("activity object", activity_object)
    if not _ACTIVITY_TYPE_RE.match(activity_type or ""):
        raise ValueError(f"not a valid activity type: {activity_type!r}")
    source = f"`{catalog}`.`{schema}`.`{activity_object}`"

    pair = _cursor_pair(after_changed_at, after_activity_id)
    # `>=` rather than the exact pair, so the floor can be pushed below the keyset branch's
    # GROUP BY and keep the aggregate off the whole table on an incremental run. An id whose
    # max is at or above the cursor keeps that same max, and one below it is dropped either
    # way. It applies to the keyset branch ONLY: see the unseen branch below for why.
    floor = f" AND hs_lastmodifieddate >= TIMESTAMP '{pair[0]}'" if pair else ""
    predicate = (
        "source_changed_at is not null"
        if pair is None
        else (
            f"(source_changed_at > TIMESTAMP '{pair[0]}' or "
            f"(source_changed_at = TIMESTAMP '{pair[0]}' and activity_id > '{pair[1]}'))"
        )
    )

    def grouped(floored: bool) -> str:
        """One row per activity at its own max modified time, optionally floored at the cursor.

        The floor goes inside the scan rather than on its output, because the scan renames
        `hs_lastmodifieddate` and a predicate naming the old column there would not bind.
        """
        scan_where = "hs_lastmodifieddate IS NOT NULL" + (floor if floored else "")
        return (
            "SELECT activity_id, max(source_changed_at) AS source_changed_at "
            "FROM ("
            "SELECT cast(hs_object_id as string) AS activity_id, "
            "hs_lastmodifieddate AS source_changed_at "
            f"FROM {source} WHERE {scan_where}"
            ") scanned "
            "WHERE activity_id IS NOT NULL GROUP BY activity_id"
        )

    ordered = "ORDER BY source_changed_at ASC, activity_id ASC"
    if pair is None:
        return (
            f"WITH worklist AS ({grouped(floored=False)}) "
            f"SELECT activity_id, source_changed_at FROM worklist WHERE {predicate} {ordered}"
        )
    # Unfloored, deliberately: a floor is exactly what hides the late arrival this branch
    # exists for. An activity behind the cursor with no landing row is the case here.
    unseen = (
        f"SELECT k.activity_id, k.source_changed_at FROM ({grouped(floored=False)}) k "
        "WHERE NOT EXISTS (SELECT 1 FROM "
        f"{landing_table} landed WHERE landed.activity_type = '{activity_type}' "
        "AND landed.activity_id = k.activity_id)"
    )
    return (
        f"WITH worklist AS ({grouped(floored=True)}), unseen AS ({unseen}) "
        f"SELECT activity_id, source_changed_at FROM worklist WHERE {predicate} "
        "UNION ALL "
        # NOT predicate, so an unseen activity that is also above the cursor is listed once.
        f"SELECT activity_id, source_changed_at FROM unseen WHERE NOT ({predicate}) "
        f"{ordered}"
    )


@dataclass(frozen=True)
class AssociationExtractionConfig:
    """Per-run values threaded through the extraction, resolved by the DAG at runtime."""

    catalog: str
    schema: str
    source_prefix: str
    api_token: str
    dag_run_id: str
    extracted_at: str
    full_sweep: bool = False


def extract_activity_associations(
    connection,
    config: AssociationExtractionConfig,
    activity_object: str,
    activity_type: str,
    session: requests.Session | None = None,
) -> dict:
    """Read one activity type's contact associations and land a snapshot per activity.

    The worklist is materialised before any HTTP call so the read cursor is closed before
    the INSERTs run on the same connection. Rows are still built and inserted one batch at
    a time, in worklist order, so a run that dies part way leaves a contiguous prefix and
    the derived cursor resumes from exactly there.
    """
    create_landing_table(connection, config.catalog, config.schema)
    landing_table = landing_relation(config.catalog, config.schema)
    after: tuple[datetime | None, str | None] = (
        (None, None)
        if config.full_sweep
        else read_cursor(connection, config.catalog, config.schema, activity_type)
    )

    sql = activity_worklist_sql(
        source_prefix=config.source_prefix,
        activity_object=activity_object,
        activity_type=activity_type,
        landing_table=landing_table,
        after_changed_at=after[0],
        after_activity_id=after[1],
    )

    activity_ids: list[str] = []
    changed_at: list[datetime] = []
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, sql)
        while True:
            chunk = cursor.fetchmany(WORKLIST_FETCH_CHUNK)
            if not chunk:
                break
            for row in chunk:
                activity_ids.append(str(row[0]))
                changed_at.append(row[1])

    batches = 0
    with_links = 0
    # A caller-provided session is the caller's to close; only one created here is closed here.
    owned_session = session is None
    http_session = session or requests.Session()
    try:
        for start in range(0, len(activity_ids), ASSOCIATION_BATCH_SIZE):
            batch = activity_ids[start : start + ASSOCIATION_BATCH_SIZE]
            associations = fetch_contact_associations(http_session, activity_object, batch, config.api_token)
            with_links += sum(1 for contact_ids in associations.values() if contact_ids)
            rows = build_snapshot_rows(
                associations,
                dict(zip(batch, changed_at[start : start + ASSOCIATION_BATCH_SIZE], strict=True)),
                activity_type,
                config.extracted_at,
                config.dag_run_id,
            )
            insert_rows(connection, config.catalog, config.schema, rows)
            batches += 1
    finally:
        if owned_session:
            http_session.close()

    logger.info(
        "%s: %d activities read in %d batch(es); %d with links, %d with none",
        activity_type,
        len(activity_ids),
        batches,
        with_links,
        len(activity_ids) - with_links,
    )
    return {
        "activity_type": activity_type,
        "activities_read": len(activity_ids),
        "batches": batches,
        # Counts only. A landing that reported zero of one is the shape a broken endpoint
        # takes, so both halves are in the summary rather than only the total.
        "activities_with_links": with_links,
        "activities_without_links": len(activity_ids) - with_links,
        # Both halves or neither: a cursor is only usable as a pair, so reporting one half
        # from this run and the other from the prior one would describe a cursor that
        # cannot be resumed from.
        "cursor_source_changed_at": format_cursor_ts(changed_at[-1]) if changed_at else None,
        "cursor_activity_id": activity_ids[-1] if activity_ids else None,
        "prior_cursor_activity_id": after[1],
    }
