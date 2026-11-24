"""Land the delegate swarm's usage ledger in the warehouse from the export's JSON Lines files.

The swarm calls Fireworks directly through pi's own provider id, and never reaches the Bifrost
gateway, so its spend cannot be reconstructed from `bifrost_request_log` at any key. Its own ledger
is upstream `agent-swarm`'s SQLite table `session_costs`, on the swarm host's EBS volume with no
published port, so a systemd timer there writes JSON Lines to S3 (``thegoodparty/ops`` owns it) and
this module lands those files in ``airflow_source``.

Two tables and not one
----------------------
``delegate_swarm_session_cost`` (one row per ``session_costs`` row) and ``delegate_swarm_task_edge``
(one row per ``agent_tasks`` row, six columns and nothing from its free text). Two tables rather than
one with a record-kind column, because the two grains have different keys and different tests, and
because the two halves of a run are two objects: see "the two files are not atomic" below.

The export/loader contract
--------------------------
**The export never rewrites a file.** Each run writes one new object per table whose name carries the
run's timestamp, holding that run's window of rows (7 days, or 90 on the monthly run), in a single
conditional PUT. Every row carries ``exported_at``. Nothing is partitioned by day and nothing is
rewritten in place, which means:

- a run's file is complete or absent, so there is no half-written day to skip;
- no completion marker is needed, because a file only appears once it is whole;
- earlier runs' files stay, so any day's numbers can be reproduced as of any run.

**COPY INTO does the landing.** It is idempotent by design: files in the source location that have
already been loaded are skipped on subsequent runs, so the loader names the prefix once and lets
COPY INTO decide what is new. That replaces a file-listing task, a per-day mapped task and an
anti-join, and it is why Airflow needs **no AWS credentials at all** here: listing and reading both
happen inside Databricks, against the one Unity Catalog storage credential.

**The two files are not atomic.** They are two objects, written in a fixed order (cost first), and a
failed run stops between them. So a cost file can land while its edge file does not, which mis-roots
that day's tasks until the next run's 7 day window brings the edges in. Both table prefixes are read
with the same COPY INTO contract, so the healing is automatic and needs nothing here.

**JSON Lines, and money as a decimal string.** The writer is a shell and ``bun`` script rather than a
Lambda with a Parquet library, so the files are ``.ndjson``: one object per row, keys exactly the
landing column names below. A JSON object is keyed, so the projection maps by name. Money crosses as
a six-decimal string and is cast here, so no value passes through a binary float.

The landing column lists are a checked-in copy of the data contract in ``02-spec.md`` section 1 of
the DATA-2843 ticket folder, and the exporter keeps the same list as a literal. Each side's test pins
its own copy, so a local edit fails a test; keeping the two in step is a stated gap that a human
closes, and the PR that changes one names the other.
"""

import contextlib
import logging
import re
from collections.abc import Sequence
from typing import NamedTuple

from include.custom_functions.bifrost_export import rows_inserted
from include.custom_functions.databricks_utils import execute_with_retry

logger = logging.getLogger("airflow.task")

SESSION_COST_TABLE_NAME = "delegate_swarm_session_cost"
TASK_EDGE_TABLE_NAME = "delegate_swarm_task_edge"

# The characters an s3 location may contain, restricted to the set that cannot close a SQL string
# literal or start a second statement. Deliberately a character-class allowlist rather than a
# reimplementation of S3's bucket-naming rules: those are a service detail that would go stale, and
# the property that matters here is only "this cannot escape the quotes".
_EXPORT_PATH_RE = re.compile(r"^s3://[A-Za-z0-9._\-]+/[A-Za-z0-9._/\-]+(?<![/])\Z")

# Path segments that mean something other than a literal key. They cannot escape the quoted literal
# (the character class above already forbids a quote, a semicolon and control characters), but `..`
# and an empty segment would silently point COPY INTO somewhere other than the prefix the Variable
# names, which is a different way to be wrong. Rejected rather than normalised: the export writes a
# plain prefix, so anything else is a configuration error worth failing on.
_EXPORT_PATH_REJECTED_SEGMENTS = frozenset({"..", "."})

# Set by the loader rather than read from the file. `loaded_at` is kept beside `exported_at` because
# the two answer different questions: "which export run last observed this row" decides which
# observation is current, and "when did this loader read it" is the pipeline's own freshness signal.
_SERVER_SET_COLUMNS: dict[str, str] = {"loaded_at": "current_timestamp()"}

# The cost table's schema, in order, and the contract with the export: 23 columns the export writes,
# plus `loaded_at` which this loader stamps.
#
# Types are Databricks SQL types. Money columns are DECIMAL, never DOUBLE: these are summed across
# every session in the mart, and binary floating point accumulates an error a finance reader would
# eventually notice. The token, duration and turn columns are counts, so BIGINT. `is_error` is the
# ledger's own 0/1 flag, landed as the integer it is rather than converted here, so the staging model
# is the one place that decides what a failed session looks like.
SESSION_COST_LANDING_COLUMNS: tuple[tuple[str, str], ...] = (
    # identity: the ledger's own key, its session, and the task and agent it belongs to
    ("id", "STRING"),
    ("session_id", "STRING"),
    ("task_id", "STRING"),
    ("agent_id", "STRING"),
    # a short operator-set label, the only non-id text in the export
    ("agent_name", "STRING"),
    ("model", "STRING"),
    # how the cost was arrived at, and which kind of row it is
    ("cost_source", "STRING"),
    ("kind", "STRING"),
    # what it cost
    ("total_cost_usd", "DECIMAL(20,6)"),
    ("harness_cost_usd", "DECIMAL(20,6)"),
    # what it used
    ("input_tokens", "BIGINT"),
    ("output_tokens", "BIGINT"),
    ("cache_read_tokens", "BIGINT"),
    ("cache_write_tokens", "BIGINT"),
    ("cache_write_5m_tokens", "BIGINT"),
    ("cache_write_1h_tokens", "BIGINT"),
    ("reasoning_output_tokens", "BIGINT"),
    ("thinking_tokens", "BIGINT"),
    ("duration_ms", "BIGINT"),
    ("num_turns", "BIGINT"),
    ("is_error", "INT"),
    # the ledger's clock, then the exporter's
    ("created_at", "TIMESTAMP"),
    ("exported_at", "TIMESTAMP"),
    # when this load read the file. Server-set, never in the file.
    ("loaded_at", "TIMESTAMP"),
)

# The edge table's schema: the six columns of the data contract, plus `loaded_at`. Both of the
# swarm's task edges are exported because upstream distinguishes them: `parent_task_id` is context
# continuity and `dispatched_by_task_id` is the task whose session dispatched this one. Which one the
# rollup follows is a dbt decision, so both have to be here or a change of mind becomes a pipeline
# change.
TASK_EDGE_LANDING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("task_id", "STRING"),
    ("parent_task_id", "STRING"),
    ("dispatched_by_task_id", "STRING"),
    ("status", "STRING"),
    # the task's clock, then the exporter's
    ("created_at", "TIMESTAMP"),
    ("exported_at", "TIMESTAMP"),
    # when this load read the file. Server-set, never in the file.
    ("loaded_at", "TIMESTAMP"),
)


def _exported(columns: tuple[tuple[str, str], ...]) -> tuple[tuple[str, str], ...]:
    """The columns the file actually carries: the landing list minus what the loader stamps."""
    return tuple((name, column_type) for name, column_type in columns if name not in _SERVER_SET_COLUMNS)


SESSION_COST_EXPORTED_COLUMNS = _exported(SESSION_COST_LANDING_COLUMNS)
TASK_EDGE_EXPORTED_COLUMNS = _exported(TASK_EDGE_LANDING_COLUMNS)


class SessionCostRow(NamedTuple):
    """One cost landing row, minus `loaded_at`, which COPY INTO stamps server-side."""

    id: str | None
    session_id: str | None
    task_id: str | None
    agent_id: str | None
    agent_name: str | None
    model: str | None
    cost_source: str | None
    kind: str | None
    total_cost_usd: str | None
    harness_cost_usd: str | None
    input_tokens: int | None
    output_tokens: int | None
    cache_read_tokens: int | None
    cache_write_tokens: int | None
    cache_write_5m_tokens: int | None
    cache_write_1h_tokens: int | None
    reasoning_output_tokens: int | None
    thinking_tokens: int | None
    duration_ms: int | None
    num_turns: int | None
    is_error: int | None
    created_at: str | None
    exported_at: str | None


class TaskEdgeRow(NamedTuple):
    """One task-edge landing row, minus `loaded_at`, which COPY INTO stamps server-side."""

    task_id: str | None
    parent_task_id: str | None
    dispatched_by_task_id: str | None
    status: str | None
    created_at: str | None
    exported_at: str | None


class LandingTable(NamedTuple):
    """One landing table: its name, its columns in order, and the row type that mirrors them."""

    name: str
    columns: tuple[tuple[str, str], ...]
    row_type: type[NamedTuple]

    @property
    def exported_columns(self) -> tuple[tuple[str, str], ...]:
        return _exported(self.columns)


SESSION_COST = LandingTable(SESSION_COST_TABLE_NAME, SESSION_COST_LANDING_COLUMNS, SessionCostRow)
TASK_EDGE = LandingTable(TASK_EDGE_TABLE_NAME, TASK_EDGE_LANDING_COLUMNS, TaskEdgeRow)

# Both, in the order a run lands them: the cost file first, so a failed run leaves the file whose
# rows the daily mart can still read.
TABLES: tuple[LandingTable, ...] = (SESSION_COST, TASK_EDGE)


class Export(NamedTuple):
    """One table and the prefix its files land from."""

    table: LandingTable
    prefix: str


def check_row_types(tables: Sequence[LandingTable]) -> None:
    """Fail the import if a row type stopped lining up with its columns.

    A field count that drifted would bind every later value one position off, which no test of either
    the columns or the row type alone would catch. Raised rather than asserted so the check survives
    `python -O`. Called at import for `TABLES`, and testable directly with a deliberately shifted
    table.
    """
    for table in tables:
        fields = tuple(name for name, _ in table.exported_columns)
        if table.row_type._fields != fields:
            raise RuntimeError(
                f"{table.row_type.__name__} fields {table.row_type._fields} must match the exported "
                f"columns {fields}, in order"
            )


check_row_types(TABLES)


def export_prefix(bucket: str, prefix: str) -> str:
    """The S3 location COPY INTO reads, without a trailing slash.

    The export writes one file per run into this prefix and never rewrites one. COPY INTO is given
    the prefix rather than a list of files, so it can compare what is there against what it has
    already loaded; that comparison is the whole reason there is no other bookkeeping here.
    """
    cleaned = prefix.strip().strip("/")
    if not cleaned:
        raise ValueError("export prefix must not be empty")
    return f"s3://{bucket}/{cleaned}"


def ensure_landing_table(connection, catalog: str, schema: str, table: LandingTable = SESSION_COST) -> None:
    """Create one append-only landing table if it is absent.

    The DAG owns this, not a migration, for the same reason the BallotReady extractor does: the
    table is owned by the service principal that writes it, and `CREATE TABLE IF NOT EXISTS` makes an
    existing one (hand-created, or from an earlier run) a no-op rather than an error.

    Identifiers cannot be bound as parameters in DDL, so they are validated instead. They come from
    Airflow Variables, which is a trusted-enough source but not a place a stray backtick should be
    able to reach a statement.
    """
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)
    columns = ", ".join(f"`{name}` {column_type}" for name, column_type in table.columns)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        execute_with_retry(
            cursor,
            f"CREATE TABLE IF NOT EXISTS {_table(catalog, schema, table)} ({columns}) CLUSTER BY AUTO",
        )


def _table(catalog: str, schema: str, table: LandingTable) -> str:
    return f"`{catalog}`.`{schema}`.`{table.name}`"


def _validate_identifier(name: str, value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"{name} is not a valid SQL identifier: {value!r}")
    return value


def build_copy_sql(catalog: str, schema: str, path: str, table: LandingTable = SESSION_COST) -> str:
    """The COPY INTO that lands whatever one prefix holds and has not landed before.

    Four choices worth stating, because each one is a failure mode:

    - **No target column list.** The mapping is the projection's aliases: without a target list,
      Databricks maps the query's output columns to the table **by name**, not by position, so each
      expression below has to keep its `as <alias>` clause naming its column. That is also why
      `loaded_at` is in the projection: it is not in the file, and only the alias binding it to
      `current_timestamp()` makes it land. `BY POSITION` is not used, and `mergeSchema` stays off: a
      file that grew a column lands without it, and a file that lost one fails here rather than
      landing a column of nulls.
    - **The path is validated, then quoted as a literal.** It cannot be a bound parameter here: the
      COPY INTO source position takes a location. Rather than ship a statement whose most dangerous
      position depends on how a parameter marker resolves, the path is checked against a strict shape
      and then interpolated as a single-quoted literal. The validation is the guard the binding would
      have been.
    - **The projection is explicit, cast to the declared types.** A value whose shape widened
      upstream then fails here with a readable message rather than landing in a column of nulls.
    - **`fileformat = json` and no `multiLine`.** Each file is JSON Lines: one object per line, no
      nesting. Databricks reads that shape with the format's default options, and its default is
      exactly one object per line (`multiLine` off). The keys become the reader's columns, so the
      projection can name them the way the Parquet loader does. Turning `multiLine` on would expect
      one array or one pretty-printed object per file and load nothing from these.
    - **No `COPY_OPTIONS ('force' = 'true')`.** That flag turns idempotency off and would reload
      every file on every run. It is the one option here that must stay false, so there is a test
      asserting it is absent.
    """
    _validate_identifier("catalog", catalog)
    _validate_identifier("schema", schema)

    select_list = []
    for name, column_type in table.columns:
        if name in _SERVER_SET_COLUMNS:
            select_list.append(f"{_SERVER_SET_COLUMNS[name]} as `{name}`")
        else:
            select_list.append(f"cast(`{name}` as {column_type}) as `{name}`")

    # No `source.` qualifier on the projection: the subquery has no alias, and `source.id` against an
    # unaliased subquery is a hard `UNRESOLVED_COLUMN` in this warehouse.
    return (
        f"copy into {_table(catalog, schema, table)}\n"
        f"from (select {', '.join(select_list)} from '{_quote_path(path)}')\n"
        f"fileformat = json"
    )


def _quote_path(path: str) -> str:
    """Validate an export path and render it as a single-quoted SQL literal.

    This is the one place a string reaches a statement as more than a bound value, so the shape is
    pinned to exactly what `export_prefix` produces: `s3://<bucket>/<key...>`, with no quote,
    whitespace, backslash or control character anywhere. A bucket name cannot contain those, and a key
    that did would be a bug on the export side worth failing on rather than quoting around.

    The path comes from Airflow Variables, which is a trusted-enough source for the catalog and schema
    names this module already interpolates; this is the same treatment, applied to the one value that
    points at cloud storage.
    """
    if not _EXPORT_PATH_RE.match(path):
        raise ValueError(f"export path is not a well-formed s3 URI: {path!r}")

    key = path.split("://", 1)[1]
    segments = key.split("/")[1:]
    if any(segment in _EXPORT_PATH_REJECTED_SEGMENTS or not segment for segment in segments):
        raise ValueError(f"export path has a relative or empty segment: {path!r}")
    return path


def land_export(connection, catalog: str, schema: str, bucket: str, prefix: str, table: LandingTable) -> int:
    """Run one COPY INTO over one export prefix, returning how many rows it loaded.

    A re-run loads nothing: COPY INTO remembers which files it has read. That is why there is no
    listing step, no per-run file bookkeeping and no anti-join here. The only thing this needs to get
    right is the prefix and the projection.
    """
    path = export_prefix(bucket, prefix)
    sql = build_copy_sql(catalog, schema, path, table)
    with contextlib.closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, sql)
        rows = cursor.fetchall() or []
        columns = [description[0] for description in (cursor.description or [])]

    count = rows_inserted(rows, columns)
    logger.info("delegate swarm: %d row(s) landed in %s from %s", count, table.name, path)
    return count


def land_exports(
    connection, catalog: str, schema: str, bucket: str, exports: Sequence[Export]
) -> dict[str, int]:
    """Create every landing table, then land every prefix, in the given order.

    Both tables are created before either is loaded: a `CREATE` that fails for the second table
    should fail the run before any file is landed, rather than halfway through. The landings then
    follow the sequence, so a failure leaves the earlier table's rows in place and the later table's
    file for the next run to pick up. COPY INTO makes a retry of the whole run cheap: what already
    landed is skipped.
    """
    for export in exports:
        ensure_landing_table(connection, catalog, schema, export.table)
    return {
        export.table.name: land_export(connection, catalog, schema, bucket, export.prefix, export.table)
        for export in exports
    }
