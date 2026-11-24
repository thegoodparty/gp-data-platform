"""Extraction helpers for the extract_hubspot_office_holders DAG.

Reads the HubSpot Office Holder custom object (object type `2-65502604`) and lands one
complete snapshot per record in `raw.hubspot.companion_office_holders`, with each
record's linked Contact and Company ids.

Three decisions shape the module:

**A full sweep every run, not a modified-since cursor.** The app upserts a record and
then creates its links in separate association calls, so a run that lands between the
two records empty arrays, and a later link change does not move `hs_lastmodifieddate`,
so nothing would ever reread that record. A cursor also never sees a deletion, and the
model is one row per live record. So every run lists every record and reads every
record's links. That is about 31 list pages plus a handful of association batches at the
object's current size. The list endpoint is used rather than search, which stops at
10,000 results.

**A partial run must never shrink the live set.** A run's rows are append-only, and a
completion marker is written to `companion_office_holders_runs` only after every record
and every link has landed. The staging model reads only the newest run that has a marker,
so a run that dies halfway leaves the previous complete snapshot in force. A marker over
a single atomic statement is impossible here: the landing spans a page loop and one
association read per direction.

**Links are read one v4 batch call per direction, taking every association type.**
`/crm/v4/associations/2-65502604/contacts/batch/read` (and the same for `companies`)
returns each linked id once with all of its association types listed. The app writes
types 76 and 73, HubSpot adds 71 and 70 for the same pair, and filtering on `typeId`
would drop a link that carries only the unlabeled type. So every `toObjectId` is taken,
whatever its type, distinct.

The HTTP plumbing here is deliberately self-contained. The companion loader's shared
client module factors it out when the other entities land; this module keeps only what
the Office Holder object needs.
"""

import json
import logging
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import closing
from datetime import UTC, datetime
from itertools import batched
from typing import Any, NamedTuple

import requests
from include.custom_functions.databricks_utils import execute_with_retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger("airflow.task")

HUBSPOT_API_BASE = "https://api.hubapi.com"

# The Office Holder object. HubSpot addresses a custom object by its id, so this string is
# what every path is built from.
OBJECT_TYPE = "2-65502604"

PAGE_LIMIT = 100
ASSOCIATION_BATCH_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
MAX_BACKOFF_SECONDS = 30.0
# A full sweep is about 31 pages today. The cap exists so a paging loop that stops
# advancing fails the run instead of spinning until the task times out.
MAX_PAGES = 2_000

LANDING_TABLE = "companion_office_holders"
RUNS_TABLE = "companion_office_holders_runs"

# The object's 31 custom properties, from the exported schema the data dictionary binds.
# Requested by name: a property omitted from the request comes back as a null column
# rather than an error, so a silent omission reads as empty data downstream.
OFFICE_HOLDER_PROPERTIES: tuple[str, ...] = (
    "br_office_holder_id",
    "br_position_id",
    "candidate_office",
    "city",
    "district",
    "elected_date",
    "gp_api_elected_office_id",
    "gp_candidacy_id",
    "gp_elected_official_term_id",
    "gp_person_id",
    "is_appointed",
    "is_judicial",
    "is_serve_icp",
    "is_vacant",
    "is_win_icp",
    "name",
    "office_email",
    "office_level",
    "office_phone",
    "office_type",
    "onboarding_completed_at",
    "party_affiliation",
    "pledged_at",
    "position_name",
    "self_reported",
    "source_systems",
    "state",
    "status",
    "sworn_in_date",
    "term_end_date",
    "term_start_date",
)

# HubSpot's own properties, asked for whatever the caller passes. `hs_merged_object_ids`
# is the only signal that a live record absorbed another one, and the two dates are the
# model's create and update columns, so none of them is optional.
SYSTEM_PROPERTIES: tuple[str, ...] = (
    "hs_createdate",
    "hs_lastmodifieddate",
    "hubspot_owner_id",
    "hs_merged_object_ids",
)

REQUESTED_PROPERTIES: tuple[str, ...] = OFFICE_HOLDER_PROPERTIES + SYSTEM_PROPERTIES

# The two object types a record can link to. Labels are not used: a record links to one
# Contact and at most one Company, and the label pair differs by which side wrote it.
ASSOCIATION_DIRECTIONS: tuple[str, ...] = ("contacts", "companies")

# One column per requested property, plus the record id and the two link arrays. Per
# property rather than one JSON payload column so that `office_email` and `office_phone`
# exist as columns a source declaration can tag, and so staging casts a named column
# instead of reading 31 JSON paths.
_ROW_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"),
    ("record_id", "STRING"),
    *((property_name, "STRING") for property_name in REQUESTED_PROPERTIES),
    ("contact_ids", "STRING"),
    ("company_ids", "STRING"),
    ("extracted_at", "TIMESTAMP"),
    ("loaded_at", "TIMESTAMP"),
)

LANDING_COLUMNS: tuple[tuple[str, str], ...] = _ROW_COLUMNS

RUNS_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "STRING"),
    ("started_at", "TIMESTAMP"),
    ("completed_at", "TIMESTAMP"),
    ("record_count", "BIGINT"),
    ("contact_link_count", "BIGINT"),
    ("company_link_count", "BIGINT"),
)

# Set server-side rather than bound, so no LandedRow field backs them.
_SERVER_SET_COLUMNS = {"loaded_at": "current_timestamp()", "completed_at": "current_timestamp()"}
_TIMESTAMP_COLUMNS = frozenset({"extracted_at", "started_at"})

INSERT_COLUMNS: tuple[str, ...] = tuple(
    name for name, _ in LANDING_COLUMNS if name not in _SERVER_SET_COLUMNS
)
RUNS_INSERT_COLUMNS: tuple[str, ...] = tuple(
    name for name, _ in RUNS_COLUMNS if name not in _SERVER_SET_COLUMNS
)

# Row-count cap per statement and a bound on the statement's total parameter characters,
# which is what the server limits rather than the parameter count.
INSERT_BATCH_ROWS = 200
MAX_INSERT_PARAM_CHARS = 800_000


class LandedRow(NamedTuple):
    """One row of the landing table, in `INSERT_COLUMNS` order."""

    run_id: str
    record_id: str
    property_values: tuple[str | None, ...]
    contact_ids: str
    company_ids: str
    extracted_at: str


def _landed_row(
    run_id: str,
    record_id: str,
    properties: Mapping[str, Any],
    contact_ids: str,
    company_ids: str,
    extracted_at: str,
) -> LandedRow:
    return LandedRow(
        run_id=run_id,
        record_id=record_id,
        property_values=tuple(_property_text(properties.get(name)) for name in REQUESTED_PROPERTIES),
        contact_ids=contact_ids,
        company_ids=company_ids,
        extracted_at=extracted_at,
    )


def _property_text(value: Any) -> str | None:
    """A property value as the text HubSpot sends.

    HubSpot returns every property as a string, so this is normally a passthrough. A null
    stays null rather than becoming the text "None": staging distinguishes an unset
    property from one holding a value, and an empty string is a real HubSpot value.
    """
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _row_values(row: LandedRow) -> tuple[Any, ...]:
    """A LandedRow as the flat binding order `INSERT_COLUMNS` names."""
    return (
        row.run_id,
        row.record_id,
        *row.property_values,
        row.contact_ids,
        row.company_ids,
        row.extracted_at,
    )


def _landed_row_fields() -> tuple[str, ...]:
    """The column names a LandedRow's fields stand for, in order.

    `property_values` is one tuple field standing for 35 columns, so the plain
    field-versus-column comparison the BallotReady module uses cannot apply here.
    """
    return (
        "run_id",
        "record_id",
        *REQUESTED_PROPERTIES,
        "contact_ids",
        "company_ids",
        "extracted_at",
    )


# A request list and a column list that stopped lining up would bind every later value one
# position off, which no test of either alone would catch. Raised rather than asserted so
# the check survives python -O.
if _landed_row_fields() != INSERT_COLUMNS:
    raise RuntimeError(
        f"landing fields {_landed_row_fields()} must match the insert columns {INSERT_COLUMNS}, in order"
    )


class HubSpotError(RuntimeError):
    """A request the loader cannot use. Never carries the token."""


class HubSpotSweepError(RuntimeError):
    """A sweep whose result cannot be trusted, so nothing may be landed."""


def _redact(message: str, secret: str | None) -> str:
    """Replace every occurrence of `secret` in `message` with a placeholder."""
    if not secret:
        return message
    return message.replace(secret, "***")


def _retry_wait(headers: Mapping[str, str], attempt: int) -> float:
    """Seconds to wait before the next attempt, honouring a numeric `Retry-After`."""
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return max(0.0, min(float(retry_after), MAX_BACKOFF_SECONDS))
        except ValueError:
            pass
    return min(2.0**attempt, MAX_BACKOFF_SECONDS)


def make_session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def request_json(
    session,
    method: str,
    path: str,
    token: str,
    *,
    query: Mapping[str, Any] | None = None,
    body: Mapping[str, Any] | None = None,
    sleep=time.sleep,
) -> dict[str, Any]:
    """One HubSpot API call, retrying 429 and 5xx, returning the decoded body.

    `token` is stripped here so a trailing newline from an Airflow Variable cannot reach
    the header, and it is redacted out of any exception text: requests embeds the offending
    header value in some of its errors, and task logs persist.
    """
    token = token.strip()
    if not token:
        raise HubSpotError("the loader token is empty or missing")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = HUBSPOT_API_BASE + path

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = session.request(
                method,
                url,
                params=dict(query) if query is not None else None,
                json=dict(body) if body is not None else None,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise HubSpotError(
                    f"{method} {path} failed after {MAX_RETRIES} retries: "
                    f"{type(exc).__name__}: {_redact(str(exc), token)}"
                ) from None
            sleep(_retry_wait({}, attempt))
            continue

        if response.status_code == 429 or response.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise HubSpotError(
                    f"{method} {path} answered {response.status_code} after {MAX_RETRIES} retries"
                )
            logger.warning(
                "%s %s answered %s (attempt %d/%d); retrying",
                method,
                path,
                response.status_code,
                attempt + 1,
                MAX_RETRIES,
            )
            sleep(_retry_wait(response.headers or {}, attempt))
            continue

        if response.status_code >= 400:
            raise HubSpotError(f"{method} {path} answered {response.status_code}: {_error_summary(response)}")

        try:
            return response.json()
        except ValueError:
            raise HubSpotError(
                f"{method} {path} answered {response.status_code} with a non-JSON body"
            ) from None

    raise HubSpotError(f"{method} {path} exhausted retries without a result")


def _error_summary(response) -> str:
    """HubSpot's own category and message, which say what a status alone does not."""
    try:
        payload = response.json()
    except ValueError:
        return "no JSON body"
    if not isinstance(payload, dict):
        return "no JSON body"
    return f"{payload.get('category')}: {payload.get('message')}"


def list_records(
    session,
    token: str,
    *,
    page_limit: int = PAGE_LIMIT,
    max_pages: int = MAX_PAGES,
) -> Iterator[dict[str, Any]]:
    """Yield every live Office Holder record, paged on `after`.

    Paged on `after` rather than by offset so a record created mid-sweep cannot shift the
    page boundaries and hide another record behind them.
    """
    query: dict[str, Any] = {
        "limit": page_limit,
        "properties": ",".join(REQUESTED_PROPERTIES),
    }
    after: str | None = None
    for _ in range(max_pages):
        page_query = dict(query)
        if after:
            page_query["after"] = after
        body = request_json(session, "GET", f"/crm/v3/objects/{OBJECT_TYPE}", token, query=page_query)
        yield from body.get("results") or []
        after = ((body.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            return
    raise HubSpotSweepError(f"list endpoint still paging after {max_pages} pages; refusing to continue")


def linked_ids(body: Mapping[str, Any]) -> dict[str, list[str]]:
    """The linked ids in one association batch response, keyed by the record id.

    Every `toObjectId` is taken whatever its association type, distinct and in the order
    HubSpot returned it. Types are ignored on purpose: a link carrying only the unlabeled
    type would be dropped by a `typeId` filter.
    """
    linked: dict[str, list[str]] = {}
    for result in body.get("results") or []:
        from_id = str((result.get("from") or {}).get("id") or "")
        if not from_id:
            continue
        ids = linked.setdefault(from_id, [])
        for association in result.get("to") or []:
            to_id = association.get("toObjectId")
            if to_id is None:
                continue
            to_id = str(to_id)
            if to_id not in ids:
                ids.append(to_id)
    return linked


def fetch_links(
    session,
    token: str,
    to_object_type: str,
    record_ids: Sequence[str],
    *,
    batch_size: int = ASSOCIATION_BATCH_SIZE,
) -> dict[str, list[str]]:
    """Linked ids per record, one v4 batch call per `batch_size` ids.

    A batch that returns any error entry fails the run rather than landing an empty array
    for that record: an empty array is a claim that the record has no links, and a failed
    batch does not know that. Failing means no completion marker, so the previous complete
    snapshot stays in force.
    """
    if to_object_type not in ASSOCIATION_DIRECTIONS:
        raise ValueError(f"unexpected association direction {to_object_type!r}")

    path = f"/crm/v4/associations/{OBJECT_TYPE}/{to_object_type}/batch/read"
    linked: dict[str, list[str]] = {}
    for batch in batched(record_ids, batch_size, strict=False):
        body = request_json(
            session, "POST", path, token, body={"inputs": [{"id": record_id} for record_id in batch]}
        )
        errors = body.get("errors") or []
        if errors:
            first = errors[0] if isinstance(errors, list) else errors
            summary = first.get("category") if isinstance(first, dict) else str(first)
            raise HubSpotSweepError(
                f"{to_object_type} association batch failed for {len(errors)} of {len(batch)} ids "
                f"({summary}); refusing to land a partial link set"
            )
        for record_id, ids in linked_ids(body).items():
            existing = linked.setdefault(record_id, [])
            for to_id in ids:
                if to_id not in existing:
                    existing.append(to_id)
    return linked


def build_landing_rows(
    records: Iterable[Mapping[str, Any]],
    links: Mapping[str, Mapping[str, Sequence[str]]],
    run_id: str,
    extracted_at: str,
) -> list[LandedRow]:
    """One row per record, with an explicit empty array for a record with no links.

    `links` is keyed by direction (`contacts`, `companies`). A missing key is a record with
    no links, which serializes as `[]`: an omitted row would read downstream as a record
    that does not exist, and a null as an unknown link set.
    """
    rows: list[LandedRow] = []
    for record in records:
        record_id = str(record.get("id") or "")
        if not record_id:
            raise HubSpotSweepError("a listed record carried no id")
        rows.append(
            _landed_row(
                run_id=run_id,
                record_id=record_id,
                properties=record.get("properties") or {},
                contact_ids=json.dumps(_link_ids(links, "contacts", record_id)),
                company_ids=json.dumps(_link_ids(links, "companies", record_id)),
                extracted_at=extracted_at,
            )
        )
    return rows


def _link_ids(links: Mapping[str, Mapping[str, Sequence[str]]], direction: str, record_id: str) -> list[str]:
    return list((links.get(direction) or {}).get(record_id) or [])


def _validate_identifier(name: str, value: str) -> str:
    """Identifiers cannot be bound as parameters in DDL, so they are validated instead."""
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def landing_relation(catalog: str, schema: str, table: str) -> str:
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)
    return f"`{catalog}`.`{schema}`.`{table}`"


def ensure_landing_tables(connection, catalog: str, schema: str) -> None:
    """Create the schema and both tables if they are absent.

    The schema is created here rather than assumed because `raw` has no `_dev` sibling: a
    dev deployment writes `dev.hubspot`, which nothing else creates.
    """
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)
    with closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        for table, columns in ((LANDING_TABLE, LANDING_COLUMNS), (RUNS_TABLE, RUNS_COLUMNS)):
            column_sql = ", ".join(f"`{name}` {column_type}" for name, column_type in columns)
            execute_with_retry(
                cursor,
                f"CREATE TABLE IF NOT EXISTS {landing_relation(catalog, schema, table)} "
                f"({column_sql}) CLUSTER BY AUTO",
            )


def _value_group(columns: Sequence[tuple[str, str]], index: int) -> str:
    values = []
    for column, _ in columns:
        if column in _SERVER_SET_COLUMNS:
            values.append(_SERVER_SET_COLUMNS[column])
        elif column in _TIMESTAMP_COLUMNS:
            values.append(f"cast(:{column}_{index} AS TIMESTAMP)")
        else:
            values.append(f":{column}_{index}")
    return "(" + ", ".join(values) + ")"


def _chunk_rows(rows: Sequence[tuple], max_chars: int = MAX_INSERT_PARAM_CHARS):
    chunk: list[tuple] = []
    chunk_chars = 0
    for row in rows:
        row_chars = sum(len(str(value)) for value in row if value is not None)
        if chunk and (chunk_chars + row_chars > max_chars or len(chunk) >= INSERT_BATCH_ROWS):
            yield chunk
            chunk, chunk_chars = [], 0
        chunk.append(row)
        chunk_chars += row_chars
    if chunk:
        yield chunk


def _insert(
    connection,
    relation: str,
    columns: Sequence[tuple[str, str]],
    insert_columns: Sequence[str],
    rows: Sequence[tuple],
    entity: str,
) -> None:
    if not rows:
        return
    column_sql = ", ".join(name for name, _ in columns)
    with closing(connection.cursor()) as cursor:
        for chunk in _chunk_rows(rows):
            value_groups = []
            parameters: dict[str, Any] = {}
            for index, row in enumerate(chunk):
                value_groups.append(_value_group(columns, index))
                parameters.update(
                    {f"{column}_{index}": value for column, value in zip(insert_columns, row, strict=True)}
                )
            sql = f"INSERT INTO {relation} ({column_sql}) VALUES " + ", ".join(value_groups)
            execute_with_retry(cursor, sql, parameters=parameters)
    logger.info("%s: landed %d rows", entity, len(rows))


def insert_landing_rows(connection, catalog: str, schema: str, rows: Sequence[LandedRow]) -> None:
    """Append one run's snapshot. Always an INSERT: staging reads the newest complete run."""
    _insert(
        connection,
        landing_relation(catalog, schema, LANDING_TABLE),
        LANDING_COLUMNS,
        INSERT_COLUMNS,
        [_row_values(row) for row in rows],
        LANDING_TABLE,
    )


def write_completion_marker(
    connection,
    catalog: str,
    schema: str,
    run_id: str,
    started_at: str,
    record_count: int,
    contact_link_count: int,
    company_link_count: int,
) -> None:
    """Write the run's completion marker. Must be the run's last write.

    Staging reads only runs that have one, so a run that dies before this leaves the
    previous complete snapshot in force and a partial sweep can never shrink the live set.
    """
    _insert(
        connection,
        landing_relation(catalog, schema, RUNS_TABLE),
        RUNS_COLUMNS,
        RUNS_INSERT_COLUMNS,
        [(run_id, started_at, record_count, contact_link_count, company_link_count)],
        RUNS_TABLE,
    )


def extract_office_holders(
    connection,
    session,
    token: str,
    catalog: str,
    schema: str,
    run_id: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Sweep the object and land one complete snapshot, marker last."""
    started_at = datetime.now(UTC).isoformat()
    extracted_at = datetime.now(UTC).isoformat()

    if not dry_run:
        ensure_landing_tables(connection, catalog, schema)

    records = list(list_records(session, token))
    if not records:
        # A sweep that returns nothing is a broken read, not an empty object: a marker over
        # it would empty the model. Refusing here keeps the previous snapshot in force.
        raise HubSpotSweepError("list endpoint returned no records; refusing to land an empty snapshot")

    record_ids = [str(record.get("id")) for record in records]
    links = {
        direction: fetch_links(session, token, direction, record_ids) for direction in ASSOCIATION_DIRECTIONS
    }

    rows = build_landing_rows(records, links, run_id, extracted_at)
    record_count = len(rows)
    contact_link_count = sum(len(ids) for ids in (links.get("contacts") or {}).values())
    company_link_count = sum(len(ids) for ids in (links.get("companies") or {}).values())

    result: dict[str, Any] = {
        "run_id": run_id,
        "record_count": record_count,
        "contact_link_count": contact_link_count,
        "company_link_count": company_link_count,
        "dry_run": dry_run,
    }
    if dry_run:
        logger.info("dry run: would land %d records; nothing written", len(rows))
        return result

    insert_landing_rows(connection, catalog, schema, rows)
    write_completion_marker(
        connection,
        catalog,
        schema,
        run_id,
        started_at,
        record_count,
        contact_link_count,
        company_link_count,
    )
    logger.info(
        "landed %d office holder records (%d contact links, %d company links) under run %s; marker written",
        record_count,
        contact_link_count,
        company_link_count,
        run_id,
    )
    return result
