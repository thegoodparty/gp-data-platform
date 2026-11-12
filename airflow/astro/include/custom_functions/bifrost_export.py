"""Land the Bifrost gateway's request log in the warehouse from the export's Parquet files.

The gateway (maximhq/bifrost, pinned at ``transports/v2.4.0``) keeps one row per model request in
its own Postgres ``logs`` table, with the tokens and the cost it computed for that request. That
table is reachable only from inside the workbench account, so a job there writes Parquet to S3
(``thegoodparty/ops`` owns it) and this module lands those files in ``airflow_source``.

The export/loader contract
--------------------------
**The export never rewrites a file.** Each run writes one new Parquet object whose name carries
the run's timestamp, holding that run's window of rows (7 days, or all 90 on the monthly run), in
a single PUT. Every row carries ``exported_at``. Nothing is partitioned by day and nothing is
rewritten in place, which means:

- a run's file is complete or absent, so there is no half-written day to skip;
- no completion marker is needed, because a file only appears once it is whole;
- earlier runs' files stay, so any day's numbers can be reproduced as of any run.

**COPY INTO does the landing.** It is idempotent by design: Databricks documents it as
"retriable and idempotent - files in the source location that have already been loaded are
skipped on subsequent runs", true "even if the files have been modified since they were loaded",
with ``COPY_OPTIONS ('force' = 'true')`` as the only way to disable that. So the loader names the
prefix once and lets COPY INTO decide what is new: a re-run loads nothing, and a new run's single
new object is the only thing picked up. That replaces a file-listing task, a per-day mapped task
and an anti-join, and it is why Airflow needs **no AWS credentials at all** here: the listing and
the reading both happen inside Databricks, against the one Unity Catalog storage credential.

**Why this and not the alternatives.** A ``dt=`` partition scheme with a completion marker needs
the writer to publish atomically and the reader to check the marker, which is two S3 permissions
for two different principals on the engineer's bucket. An append-only file per run needs one.

Reading the gateway's database
------------------------------
The RDS instance is a ``t4g.micro`` behind the one gateway every engineer codes through, which
bounds what the export may do: a predicate on ``timestamp`` (so it reads through
``idx_logs_timestamp`` rather than scanning), only the projected columns, and an off-peak hour.
Those are requirements on the export, recorded in the spec's ask list.
"""

import contextlib
import logging
import re
from typing import NamedTuple

from include.custom_functions.databricks_utils import execute_with_retry

logger = logging.getLogger("airflow.task")

TABLE_NAME = "bifrost_request_log"

# The characters an s3 location may contain, restricted to the set that cannot close a SQL string
# literal or start a second statement. Deliberately a character-class allowlist rather than a
# reimplementation of S3's bucket-naming rules: those are a service detail that would go stale, and
# the property that matters here is only "this cannot escape the quotes".
_EXPORT_PATH_RE = re.compile(r"^s3://[A-Za-z0-9._\-]+/[A-Za-z0-9._/\-]+(?<![/])\Z")

# The export's projection and this table's schema, in table order. One constant so the DDL, the
# COPY INTO column list and the cross-subproject drift test cannot disagree; a column added to
# one and not the others silently shifts every later value by a position.
#
# Types are Databricks SQL types. Money columns are DECIMAL, never DOUBLE: these are summed
# across hundreds of thousands of rows in the mart, and binary floating point accumulates an
# error a finance reader would eventually notice. `latency` is a measurement, not money.
#
# `exported_at` comes from the file and `loaded_at` is stamped here. Both are kept because they
# answer different questions: "which export run last observed this row" decides which observation
# is current, and "when did this loader read it" is the pipeline's own freshness signal.
LANDING_COLUMNS: tuple[tuple[str, str], ...] = (
    # identity and the two source clocks, plus the export's own stamp
    ("id", "STRING"),
    ("timestamp", "TIMESTAMP"),
    ("created_at", "TIMESTAMP"),
    ("exported_at", "TIMESTAMP"),
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
    # when this load read the file. Server-set, never in the Parquet.
    ("loaded_at", "TIMESTAMP"),
)

# Set by the loader rather than read from the file.
_SERVER_SET_COLUMNS: dict[str, str] = {"loaded_at": "current_timestamp()"}

# The columns the file actually carries.
EXPORTED_COLUMNS: tuple[tuple[str, str], ...] = tuple(
    (name, column_type) for name, column_type in LANDING_COLUMNS if name not in _SERVER_SET_COLUMNS
)


class BifrostRow(NamedTuple):
    """One landing row, minus `loaded_at`, which COPY INTO stamps server-side."""

    id: str | None
    timestamp: str | None
    created_at: str | None
    exported_at: str | None
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
if BifrostRow._fields != tuple(name for name, _ in EXPORTED_COLUMNS):
    raise RuntimeError(
        f"BifrostRow fields {BifrostRow._fields} must match the exported columns "
        f"{tuple(name for name, _ in EXPORTED_COLUMNS)}, in order"
    )


def export_prefix(bucket: str, prefix: str) -> str:
    """The S3 location COPY INTO reads, without a trailing slash.

    The export writes one file per run into this prefix and never rewrites one. COPY INTO is
    given the prefix rather than a list of files, so it can compare what is there against what it
    has already loaded; that comparison is the whole reason there is no other bookkeeping here.
    """
    cleaned = prefix.strip().strip("/")
    if not cleaned:
        raise ValueError("export prefix must not be empty")
    return f"s3://{bucket}/{cleaned}"


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
    table = _table(catalog, schema)
    columns = ", ".join(f"`{name}` {column_type}" for name, column_type in LANDING_COLUMNS)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        execute_with_retry(cursor, f"CREATE TABLE IF NOT EXISTS {table} ({columns}) CLUSTER BY AUTO")


def _table(catalog: str, schema: str) -> str:
    return f"`{catalog}`.`{schema}`.`{TABLE_NAME}`"


def _validate_identifier(name: str, value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def build_copy_sql(catalog: str, schema: str, path: str) -> str:
    """The COPY INTO that lands whatever the prefix holds and has not landed before.

    Three choices worth stating, because each one is a failure mode:

    - **The path is validated, then quoted as a literal.** It cannot be a bound parameter here: the
      COPY INTO source position takes a location, and while Databricks does have parameter markers
      (`:path` raises `UNBOUND_SQL_PARAMETER` when unbound, so the feature exists) nothing
      establishes that COPY INTO resolves one as a source location rather than a table reference.
      Rather than ship a statement whose most dangerous position depends on that, the path is
      checked against a strict shape and then interpolated as a single-quoted literal. The
      validation is the guard the binding would have been.
    - **The projection is explicit, cast to the declared types.** A Parquet type that widens
      upstream then fails here with a readable message rather than landing a column of nulls.
    - **No `COPY_OPTIONS ('force' = 'true')`.** That flag turns idempotency off and would reload
      every file on every run. It is the one option here that must stay false, so there is a test
      asserting it is absent.
    """
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)
    table = _table(catalog, schema)

    select_list = []
    for name, column_type in LANDING_COLUMNS:
        if name in _SERVER_SET_COLUMNS:
            select_list.append(f"{_SERVER_SET_COLUMNS[name]} as `{name}`")
        else:
            select_list.append(f"cast(`{name}` as {column_type}) as `{name}`")

    target = ", ".join(f"`{name}`" for name, _ in LANDING_COLUMNS)

    # No `source.` qualifier on the projection: the subquery has no alias, and `source.id` against
    # an unaliased subquery is a hard `UNRESOLVED_COLUMN` in this warehouse (verified).
    return (
        f"copy into {table} ({target})\n"
        f"from (select {', '.join(select_list)} from '{_quote_path(path)}')\n"
        f"fileformat = parquet"
    )


def _quote_path(path: str) -> str:
    """Validate an export path and render it as a single-quoted SQL literal.

    This is the one place a string reaches a statement as more than a bound value, so the shape is
    pinned to exactly what `export_prefix` produces: `s3://<bucket>/<key...>`, with no quote,
    whitespace, backslash or control character anywhere. A bucket name cannot contain those, and a
    key that did would be a bug on the export side worth failing on rather than quoting around.

    The path comes from Airflow Variables, which is a trusted-enough source for the catalog and
    schema names this module already interpolates; this is the same treatment, applied to the one
    value that points at cloud storage.
    """
    if not _EXPORT_PATH_RE.match(path):
        raise ValueError(f"export path is not a well-formed s3 URI: {path!r}")
    return path


def land_export(connection, catalog: str, schema: str, bucket: str, prefix: str) -> int:
    """Run one COPY INTO over the export prefix, returning how many rows it loaded.

    A re-run loads nothing: COPY INTO remembers which files it has read. That is why there is no
    listing step, no per-run file bookkeeping and no anti-join here — the only thing this needs to
    get right is the prefix and the projection.
    """
    path = export_prefix(bucket, prefix)
    sql = build_copy_sql(catalog, schema, path)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, sql)
        rows = cursor.fetchall() or []
        columns = [description[0] for description in (cursor.description or [])]

    count = rows_inserted(rows, columns)
    logger.info("bifrost: %d row(s) landed from %s", count, path)
    return count


def rows_inserted(rows, columns) -> int:
    """Total rows loaded from a COPY INTO result set.

    The count column is read by name, never by position: the result also carries
    `num_skipped_corrupt_files` and friends, and positions are not something to hard-code against a
    service that may reorder them.

    Whether COPY INTO returns one row per file or a single summary row is not something I could
    confirm without a real export, so this sums across whatever rows come back rather than assuming
    either. A re-run that skips every file reports zero, and that is the normal steady state rather
    than a failure, so zero is a legitimate answer — but a *missing* count column is not, and raises.
    """
    if not rows:
        return 0
    index = None
    for position, name in enumerate(columns):
        if str(name).lower() in {"rows_inserted", "num_inserted_rows"}:
            index = position
            break
    if index is None:
        # Not a zero. A result shape we do not recognise is a change to the statement's contract,
        # and reporting it as "no new rows" is how a broken load stays green.
        raise ValueError(f"COPY INTO returned columns {columns} with no rows_inserted; cannot report a count")
    return sum(int(row[index] or 0) for row in rows)
