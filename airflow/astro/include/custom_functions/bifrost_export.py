"""Load the Bifrost gateway's request log into the warehouse.

The gateway (maximhq/bifrost, pinned at ``transports/v2.4.0``) keeps one row per model request
in its own Postgres ``logs`` table, with the tokens and the cost it computed for that request.
That table is reachable only from inside the workbench account, so the data reaches us as a
daily Parquet export an in-VPC job writes to S3 (``thegoodparty/ops`` owns that job). This
module is the other half: Parquet in S3 to an append-only landing table in ``airflow_source``.

The export/loader contract
--------------------------
**Layout.** ``s3://<bucket>/<prefix>/dt=<YYYY-MM-DD>/part-*.parquet``, one partition per UTC day
of ``timestamp``, plus ``dt=<YYYY-MM-DD>/_SUCCESS`` written only once that day is complete. A
day without the marker is skipped: an export run re-emits whole days, so a load that overlaps
one would otherwise read a half-written partition, and nothing on the data side would notice.

**Projection.** ``LANDING_COLUMNS`` below is the contract. It is 28 of the source table's 121
columns; the rest are payload and debug columns (``input_history``, ``output_message``,
``token_usage``, ``raw_response`` and friends) that content logging being off makes empty today
and that we do not want to start carrying if that setting ever changes. The export's read-only
Postgres role holds a *column-level* SELECT over exactly these, so the projection holds at the
source rather than only in the query.

**Why the insert is a no-op for unchanged rows.** A gateway row is written ``status =
'processing'`` and later updated in place to a terminal status with its cost and tokens. So the
export re-emits a trailing window rather than only new rows, or it would capture requests
mid-flight and never see what they cost. That means the same request is presented again on every
run, which is why the insert is guarded by an anti-join on ``(id, status, cost)``: a row observed
again unchanged is not inserted again, and a row that changed (a new status, a repriced cost) is.
The table therefore grows with *changes* rather than with windows, and it stays append-only: no
DELETE, no UPDATE, no MERGE in place, so delta history shows nothing was rewritten.

``cost <=> s.cost`` is Databricks' null-safe equality. A plain ``=`` never matches NULL, so every
unpriced ``processing`` row would be re-inserted on every run.

Reading the gateway's database
------------------------------
The RDS instance is a ``t4g.micro`` behind the one gateway every engineer codes through, which
bounds what the export may do: a predicate on ``timestamp`` (so it reads through
``idx_logs_timestamp`` rather than scanning), only the projected columns, and an off-peak hour.
That last is why the export runs about 09:00 UTC, which is 1-2am Pacific, rather than at 00:02
UTC, which is 4pm Pacific.
"""

import contextlib
import logging
from typing import NamedTuple

from include.custom_functions.databricks_utils import execute_with_retry

logger = logging.getLogger("airflow.task")

TABLE_NAME = "bifrost_request_log"

# The export's projection and this table's schema, in table order. One constant so the DDL,
# the INSERT column list and the cross-subproject drift test cannot disagree; a column added
# to one and not the others would silently shift every later value by a position.
#
# Types are Databricks SQL types. Money columns are DECIMAL, never DOUBLE: these are summed
# across hundreds of thousands of rows in the mart, and binary floating point accumulates an
# error a finance reader would eventually notice. `latency` is a measurement, not money.
LANDING_COLUMNS: tuple[tuple[str, str], ...] = (
    # identity and the two clocks
    ("id", "STRING"),
    ("timestamp", "TIMESTAMP"),
    ("created_at", "TIMESTAMP"),
    # what was called and how it went
    ("provider", "STRING"),
    ("model", "STRING"),
    ("canonical_model_name", "STRING"),
    ("alias", "STRING"),
    ("object_type", "STRING"),
    ("status", "STRING"),
    ("is_passthrough", "BOOLEAN"),
    ("fallback_index", "INT"),
    ("parent_request_id", "STRING"),
    # who called it, and from what
    ("app", "STRING"),
    ("user_agent", "STRING"),
    ("virtual_key_id", "STRING"),
    ("virtual_key_name", "STRING"),
    ("team_id", "STRING"),
    ("team_name", "STRING"),
    ("user_id", "STRING"),
    ("user_name", "STRING"),
    # what it cost
    ("cost", "DECIMAL(20,6)"),
    ("input_cost", "DECIMAL(20,6)"),
    ("output_cost", "DECIMAL(20,6)"),
    ("additional_cost", "DECIMAL(20,6)"),
    # what it used
    ("prompt_tokens", "BIGINT"),
    ("completion_tokens", "BIGINT"),
    ("total_tokens", "BIGINT"),
    ("cached_read_tokens", "BIGINT"),
    ("latency", "DOUBLE"),
    # when this load observed it. Server-set, never bound: it is the loader's own clock, and
    # the staging model's dedupe orders on it.
    ("loaded_at", "TIMESTAMP"),
)

# Set by the INSERT rather than read from the file, so it never collides with a source column.
_SERVER_SET_COLUMNS: dict[str, str] = {"loaded_at": "current_timestamp()"}

INSERT_COLUMNS: tuple[str, ...] = tuple(
    name for name, _ in LANDING_COLUMNS if name not in _SERVER_SET_COLUMNS
)

# The columns the anti-join compares to decide whether a row is new. Together with `id` these
# are exactly the columns that change when a request finalizes or is repriced, so comparing them
# is what makes "unchanged" mean unchanged rather than "looks the same at a glance".
_DEDUPE_KEY: tuple[str, ...] = ("status", "cost")


class BifrostRow(NamedTuple):
    """One landing row, minus `loaded_at`, which the INSERT stamps server-side."""

    id: str | None
    timestamp: str | None
    created_at: str | None
    provider: str | None
    model: str | None
    canonical_model_name: str | None
    alias: str | None
    object_type: str | None
    status: str | None
    is_passthrough: bool | None
    fallback_index: int | None
    parent_request_id: str | None
    app: str | None
    user_agent: str | None
    virtual_key_id: str | None
    virtual_key_name: str | None
    team_id: str | None
    team_name: str | None
    user_id: str | None
    user_name: str | None
    cost: str | None
    input_cost: str | None
    output_cost: str | None
    additional_cost: str | None
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    cached_read_tokens: int | None
    latency: float | None


# A BifrostRow field that stopped lining up with its column would bind every later value one
# position off, which no test of either alone would catch. Raised rather than asserted so the
# check survives `python -O`.
if BifrostRow._fields != INSERT_COLUMNS:
    raise RuntimeError(
        f"BifrostRow fields {BifrostRow._fields} must match the landing columns {INSERT_COLUMNS}, in order"
    )

# Bound as strings because the connector handles them predictably that way; the INSERT casts
# them back, so the column type is stated once in LANDING_COLUMNS rather than twice.
_TIMESTAMP_COLUMNS = frozenset({"timestamp", "created_at"})


def partition_path(bucket: str, prefix: str, partition_day: str) -> str:
    """The S3 path of one day's Parquet files, without a trailing slash.

    `partition_day` is an ISO date. It is interpolated into a path rather than bound, so it is
    validated first: this string reaches the warehouse inside a `read_files()` call.
    """
    if not _is_iso_date(partition_day):
        raise ValueError(f"partition day is not an ISO date: {partition_day!r}")
    return f"s3://{bucket}/{prefix.strip('/')}/dt={partition_day}"


def success_marker_path(bucket: str, prefix: str, partition_day: str) -> str:
    """The completion marker for one day. Its presence is what makes a partition readable."""
    return f"{partition_path(bucket, prefix, partition_day)}/_SUCCESS"


def _is_iso_date(value: str) -> bool:
    if not isinstance(value, str) or len(value) != 10 or value[4] != "-" or value[7] != "-":
        return False
    return value.replace("-", "").isdigit()


def ensure_landing_table(connection, catalog: str, schema: str) -> None:
    """Create the append-only landing table if it is absent.

    The DAG owns this, not a migration, for the same reason the BallotReady extractor does: the
    table is owned by the service principal that writes it, and `CREATE TABLE IF NOT EXISTS`
    makes an existing one (hand-created, or from an earlier run) a no-op rather than an error.

    Identifiers cannot be bound as parameters in DDL, so they are validated instead. They come
    from Airflow Variables, which is a trusted-enough source but not a place a stray backtick
    should be able to reach a statement.
    """
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)
    table = f"`{catalog}`.`{schema}`.`{TABLE_NAME}`"
    columns = ", ".join(f"`{name}` {column_type}" for name, column_type in LANDING_COLUMNS)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        execute_with_retry(cursor, f"CREATE TABLE IF NOT EXISTS {table} ({columns}) CLUSTER BY AUTO")


def _validate_identifier(name: str, value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def build_insert_sql(catalog: str, schema: str, source_path: str) -> str:
    """The INSERT for one day's Parquet files.

    The anti-join is the whole point of this statement rather than a `CREATE OR REPLACE`: the
    landing table is append-only so that history survives, and the window is re-emitted on every
    run, so unchanged rows must be filtered out rather than allowed to accumulate.

    `read_files` is given an explicit schema so a Parquet type that widens upstream (or a file
    written by a different writer version) fails at this statement with a readable message
    instead of landing a column of nulls. It is also given the file list rather than a directory
    so the loader never picks up a `_SUCCESS` marker as data.
    """
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)
    table = f"`{catalog}`.`{schema}`.`{TABLE_NAME}`"

    select_list = []
    for name, column_type in LANDING_COLUMNS:
        if name in _SERVER_SET_COLUMNS:
            select_list.append(f"{_SERVER_SET_COLUMNS[name]} as `{name}`")
        else:
            select_list.append(f"cast(source.`{name}` as {column_type}) as `{name}`")

    schema_clause = ", ".join(f"`{name}` {column_type}" for name, column_type in LANDING_COLUMNS)
    match = " and ".join(
        ["landed.`id` = source.`id`"] + [f"landed.`{column}` <=> source.`{column}`" for column in _DEDUPE_KEY]
    )
    columns = ", ".join(f"`{name}`" for name in INSERT_COLUMNS)

    return (
        f"insert into {table} ({columns})\n"
        f"select {', '.join(select_list)}\n"
        f"from read_files(:path, format => 'parquet', schema => '{schema_clause}') as source\n"
        f"where not exists (\n"
        f"    select 1 from {table} as landed where {match}\n"
        f")"
    )


def insert_day(
    connection,
    catalog: str,
    schema: str,
    bucket: str,
    prefix: str,
    partition_day: str,
    s3_conn=None,
) -> int:
    """Insert one day's new rows, returning how many were inserted.

    The caller has already established that the day is complete (its `_SUCCESS` marker exists)
    and inside the window; this function does not re-check either, so that the decisions live in
    one place and are testable without a cursor.
    """
    path = partition_path(bucket, prefix, partition_day)
    sql = build_insert_sql(catalog, schema, path)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, sql, parameters={"path": path})
        row = cursor.fetchone()
    inserted = int(row[0]) if row and row[0] is not None else 0
    logger.info("bifrost %s: %d new row(s) landed", partition_day, inserted)
    return inserted


def days_in_window(days: list[str], lookback_days: int, today: str, full_reload: bool = False) -> list[str]:
    """The partitions to load, newest first.

    Every marked day inside the window, every run. There is deliberately no "newest landed"
    watermark: the export re-emits whole days, so filtering by what has already landed would
    skip the re-emitted copy of a day and a request first seen `processing` would never be
    re-read as its terminal self. Re-loading is cheap because the INSERT's anti-join drops the
    rows that did not change.
    """
    if not _is_iso_date(today):
        raise ValueError(f"today is not an ISO date: {today!r}")
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be >= 1, got {lookback_days}")

    selected = []
    for day in days:
        if not _is_iso_date(day) or day > today:
            continue
        if full_reload or _days_between(day, today) < lookback_days:
            selected.append(day)
    return sorted(set(selected), reverse=True)


def _days_between(earlier: str, later: str) -> int:
    """Whole days from `earlier` to `later`, both ISO dates.

    Parsed by hand rather than through `datetime` so the arithmetic cannot pick up a local
    timezone: these are partition labels in UTC, and the window is a count of them.
    """
    from datetime import date

    y1, m1, d1 = (int(part) for part in earlier.split("-"))
    y2, m2, d2 = (int(part) for part in later.split("-"))
    return (date(y2, m2, d2) - date(y1, m1, d1)).days
