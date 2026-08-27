"""Pre-swap quality gates and the dated-vintage swap for the matcha ER outputs.

The matcha container writes each run to a DATED table
(`er_source.clustered_candidacy_stages_20260825`) and never to a live one: its
upload is `CREATE OR REPLACE TABLE` followed by `COPY INTO`, so pointing it at
a live table means a mid-upload failure leaves every downstream dbt model
reading an empty or partial table. This module is the other half of that
contract — it gates the dated table, then renames it into the live name.

Unity Catalog has no multi-statement transaction, so the swap is an explicit
sequence rather than one atomic statement. It is idempotent for a crash
WITHIN one table's three statements — the leftover `_old` from a crashed run
is dropped first, so a crash never wedges the next attempt. It is NOT
idempotent across a full table promotion: a completed swap CONSUMES the dated
table (it becomes the live table), so `swap_table` guards on the dated
table's existence before doing anything, rather than re-running drop+rename
against a table that is already gone.

Everything above the "Databricks execution" divider is pure, so the gate logic
is testable without a warehouse.
"""

import logging
import re
from dataclasses import dataclass

from airflow.sdk import BaseHook, Variable
from include.custom_functions.databricks_utils import execute_with_retry, get_databricks_connection

logger = logging.getLogger("airflow.task")


@dataclass(frozen=True)
class TableGate:
    """Pre-swap gate for one er_source table.

    Cluster and pairwise tables are deliberately gated differently. The civics
    marts build their crosswalks from the cluster tables, so those get identity
    and source-coverage checks. Pairwise is audit-only and its row volume
    swings legitimately with blocking and threshold tuning, so a gate as tight
    as the cluster one would fail on ordinary model work and train us to
    ignore it.
    """

    # Minimum plausible row count when no prior live table exists.
    cold_start_floor: int
    # Refuse the swap when dated/live row count falls below this ratio.
    min_prior_ratio: float
    # NULL probes over the dated rows.
    not_null_columns: tuple[str, ...]
    # Identity column for the distinct and overlap checks. None skips both.
    id_column: str | None = None
    # Floor on dated-vs-live id overlap; catches a wholesale re-key rather than
    # a refresh. Requires id_column. None skips the check.
    min_id_overlap: float | None = None
    # Every source that must still be represented. Empty skips the check.
    expected_sources: tuple[str, ...] = ()
    source_column: str = "source_name"


@dataclass(frozen=True)
class EntitySpec:
    """One matcha entity type and the two er_source tables it produces."""

    entity_type: str
    # Plural stem shared by both output tables.
    table_stem: str
    prematch_model: str
    cluster_gate: TableGate
    pairwise_gate: TableGate

    @property
    def cluster_table(self) -> str:
        return f"clustered_{self.table_stem}"

    @property
    def pairwise_table(self) -> str:
        return f"pairwise_{self.table_stem}"


_CLUSTER_NOT_NULL = ("cluster_id", "unique_id")
_PAIRWISE_NOT_NULL = ("unique_id_l", "unique_id_r")

# Cold-start floors sit at roughly 70% of the live counts observed when this
# was written (563k candidacy, 554k elected official, 731k election stage) —
# low enough not to trip on ordinary growth, high enough to catch a run that
# produced almost nothing.
ENTITIES: tuple[EntitySpec, ...] = (
    EntitySpec(
        entity_type="candidacy_stage",
        table_stem="candidacy_stages",
        prematch_model="int__er_prematch_candidacy_stages",
        cluster_gate=TableGate(
            cold_start_floor=400_000,
            min_prior_ratio=0.8,
            not_null_columns=_CLUSTER_NOT_NULL,
            id_column="unique_id",
            min_id_overlap=0.8,
            expected_sources=("ballotready", "techspeed", "ddhq", "gp_api"),
        ),
        pairwise_gate=TableGate(
            cold_start_floor=1_000,
            min_prior_ratio=0.5,
            not_null_columns=_PAIRWISE_NOT_NULL,
        ),
    ),
    EntitySpec(
        entity_type="elected_official",
        table_stem="elected_officials",
        prematch_model="int__er_prematch_elected_officials",
        cluster_gate=TableGate(
            cold_start_floor=400_000,
            min_prior_ratio=0.8,
            not_null_columns=_CLUSTER_NOT_NULL,
            id_column="unique_id",
            min_id_overlap=0.8,
            expected_sources=("ballotready_techspeed", "gp_api", "ddhq"),
        ),
        # This entity's pairwise output is tiny (about 1.8k rows) next to its
        # 554k clusters, so its cold-start floor is lower than the others'.
        pairwise_gate=TableGate(
            cold_start_floor=500,
            min_prior_ratio=0.5,
            not_null_columns=_PAIRWISE_NOT_NULL,
        ),
    ),
    EntitySpec(
        entity_type="election_stage",
        table_stem="election_stages",
        prematch_model="int__er_prematch_election_stages",
        cluster_gate=TableGate(
            cold_start_floor=500_000,
            min_prior_ratio=0.8,
            not_null_columns=_CLUSTER_NOT_NULL,
            id_column="unique_id",
            min_id_overlap=0.8,
            expected_sources=("ballotready", "ddhq", "techspeed"),
        ),
        pairwise_gate=TableGate(
            cold_start_floor=1_000,
            min_prior_ratio=0.5,
            not_null_columns=_PAIRWISE_NOT_NULL,
        ),
    ),
)


def _ident(part: str) -> str:
    """Backtick-quote one identifier, refusing anything that could break out.

    Catalog and schema come from Airflow Variables, so they are operator input
    rather than constants; every identifier reaches SQL by interpolation.
    """
    if not part or "`" in part:
        raise ValueError(f"Unsafe Databricks identifier: {part!r}")
    return f"`{part}`"


def fqn(catalog: str, schema: str, table: str) -> str:
    """Fully-qualified, quoted Unity Catalog table name."""
    return f"{_ident(catalog)}.{_ident(schema)}.{_ident(table)}"


def dated_name(table: str, run_date: str) -> str:
    """Vintage table name matcha writes for this run (`<table>_<yyyymmdd>`)."""
    return f"{table}_{run_date}"


def old_name(table: str) -> str:
    """Renamed-aside name the live table takes during a swap."""
    return f"{table}_old"


SWAP_GATE_VARIABLE = "matcha_swap_enabled"


def swap_enabled() -> bool:
    """Whether the swap step may rename a dated vintage into the live name.

    Case-sensitive and exact on purpose: only the literal string "true" arms
    it, so an unset, empty, or differently-cased Variable value fails safely
    into rehearsal (match + gate still run) rather than going live by
    accident. Reads a Variable, so callable at task runtime only.
    """
    return Variable.get(SWAP_GATE_VARIABLE, default="") == "true"


# ── Gate checks (pure) ──


def check_counts(loaded_count: int, prior_count: int, gate: TableGate, table: str) -> None:
    """Ratio floor against the prior live table, cold-start floor without one."""
    if prior_count > 0:
        ratio = loaded_count / prior_count
        if ratio < gate.min_prior_ratio:
            raise ValueError(
                f"{table}: loaded {loaded_count} rows, prior live had "
                f"{prior_count} (ratio {ratio:.2f}, floor {gate.min_prior_ratio}) "
                f"— refusing to swap"
            )
    elif loaded_count < gate.cold_start_floor:
        raise ValueError(
            f"{table}: cold-start load of {loaded_count} rows "
            f"(<{gate.cold_start_floor}) is implausibly small — refusing to swap"
        )


def check_distinct_ids(loaded_count: int, distinct_count: int, gate: TableGate, table: str) -> None:
    """The cluster tables promise one row per identity. No id column skips it."""
    if gate.id_column is None:
        return
    if distinct_count != loaded_count:
        raise ValueError(
            f"{table}: {loaded_count - distinct_count} duplicate "
            f"{gate.id_column} values in {loaded_count} rows — refusing to swap"
        )


def check_id_overlap(overlap: int, prior_count: int, gate: TableGate, table: str) -> None:
    """Too few shared ids means the run re-keyed wholesale rather than refreshed."""
    if gate.min_id_overlap is None or prior_count <= 0:
        return
    if overlap / prior_count < gate.min_id_overlap:
        raise ValueError(
            f"{table}: dated id overlap {overlap}/{prior_count} below floor "
            f"{gate.min_id_overlap}; wholesale re-key suspected — refusing to swap"
        )


def check_nulls(null_rows: int, gate: TableGate, table: str) -> None:
    """NULL probe over the dated rows."""
    if null_rows > 0:
        raise ValueError(
            f"{table}: {null_rows} rows have a NULL in " f"{list(gate.not_null_columns)} — refusing to swap"
        )


def check_sources(found_sources: set[str], gate: TableGate, table: str) -> None:
    """Every expected source must still be represented.

    A source dropping out of prematch is silent otherwise: the row count barely
    moves and the clusters just quietly stop bridging that source.
    """
    if not gate.expected_sources:
        return
    missing = sorted(set(gate.expected_sources) - found_sources)
    if missing:
        raise ValueError(
            f"{table}: expected sources {missing} absent from " f"{gate.source_column} — refusing to swap"
        )


# ── SQL builders (pure) ──

_VINTAGE_SUFFIX = re.compile(r"^(?P<table>.+)_(?P<vintage>\d{8})$")


def count_sql(target: str) -> str:
    """Row count of a fully-qualified table."""
    return f"SELECT count(*) FROM {target}"


def distinct_count_sql(target: str, column: str) -> str:
    """Distinct values of one column."""
    return f"SELECT count(DISTINCT {_ident(column)}) FROM {target}"


def null_probe_sql(target: str, columns: tuple[str, ...]) -> str:
    """Count rows carrying a NULL in ANY of `columns`."""
    predicate = " OR ".join(f"{_ident(c)} IS NULL" for c in columns)
    return f"SELECT count(*) FROM {target} WHERE {predicate}"


def overlap_sql(dated: str, live: str, column: str) -> str:
    """Count ids present in BOTH the dated and the live table."""
    col = _ident(column)
    return (
        f"SELECT count(*) FROM (SELECT DISTINCT {col} FROM {dated}) d "
        f"JOIN (SELECT DISTINCT {col} FROM {live}) l ON d.{col} = l.{col}"
    )


def distinct_sources_sql(target: str, column: str) -> str:
    """Every distinct source value present in the dated table."""
    return f"SELECT DISTINCT {_ident(column)} FROM {target}"


def swap_statements(
    catalog: str,
    schema: str,
    live_table: str,
    dated_table: str,
    live_exists: bool,
) -> list[str]:
    """Ordered statements that promote a dated vintage into the live name.

    Unity Catalog gives no multi-statement transaction, so the sequence is
    explicit. It is idempotent for a crash WITHIN these three statements: the
    pre-drop MUST come first, since a crash between the swap and cleanup
    leaves an `_old` table behind, and the next attempt's rename-aside would
    collide with it — failing run after run until a human intervened. It is
    NOT idempotent once the final rename has committed: the dated table no
    longer exists at that point, and building this same statement list again
    would fail on a missing table. Callers must not invoke this a second time
    for a table already promoted — `swap_table` enforces that by checking the
    dated table's existence first.
    """
    aside = old_name(live_table)
    statements = [f"DROP TABLE IF EXISTS {fqn(catalog, schema, aside)}"]
    if live_exists:
        statements.append(
            f"ALTER TABLE {fqn(catalog, schema, live_table)} RENAME TO " f"{fqn(catalog, schema, aside)}"
        )
    statements.append(
        f"ALTER TABLE {fqn(catalog, schema, dated_table)} RENAME TO " f"{fqn(catalog, schema, live_table)}"
    )
    return statements


def stale_vintages(existing_tables: list[str], table: str, cutoff: str) -> list[str]:
    """Dated vintages of `table` older than `cutoff` (a yyyymmdd string).

    Matches only an exact `<table>_<8 digits>` suffix, so `clustered_x` never
    sweeps up `clustered_xyz_20260701`, the live table, or `<table>_old`.
    Vintages are zero-padded yyyymmdd, so a lexical compare is a date compare.
    """
    stale = []
    for name in existing_tables:
        match = _VINTAGE_SUFFIX.match(name)
        if match and match.group("table") == table and match.group("vintage") < cutoff:
            stale.append(name)
    return sorted(stale)


# ── Databricks execution ──
#
# Everything below opens or uses a warehouse connection. Keep it thin: assemble
# with the builders above, run, then hand the numbers to the pure checks.


def databricks_conn_fields(databricks_conn_id_var: str = "databricks_conn_id") -> dict[str, str]:
    """The host and OAuth credentials of the deployment's Databricks connection.

    WHICH connection is chosen at task runtime from the shared
    `databricks_conn_id` Variable (`databricks_dev` on dev, `databricks` on
    prod), matching the other DAGs. Must not be called at DAG parse.

    One accessor for both consumers — the gate/swap tasks' own warehouse
    connection and the match pod's environment — so the two cannot drift on
    which fields they require or how they read them.
    """
    conn_id = Variable.get(databricks_conn_id_var, default="databricks")
    db_conn = BaseHook.get_connection(conn_id)
    http_path = db_conn.extra_dejson.get("http_path", "")
    if not (db_conn.host and db_conn.login and db_conn.password and http_path):
        raise ValueError(
            f"Databricks connection '{conn_id}' is missing a required "
            "host, login, password, or http_path (extra) field"
        )
    return {
        "host": db_conn.host,
        "http_path": http_path,
        "client_id": db_conn.login,
        "client_secret": db_conn.password,
    }


def pod_databricks_env(databricks_conn_id_var: str = "databricks_conn_id") -> dict[str, str]:
    """The DATABRICKS_* variables the matcha container authenticates with.

    Read at task runtime rather than templated into the operator, so the
    credentials are never a rendered template value: Airflow snapshots
    rendered fields (and the KPO pod YAML) into the metadata DB before
    `pre_execute` runs, and while it redacts known secrets on the way in,
    values that never reach the snapshot need no redaction to be safe.
    """
    fields = databricks_conn_fields(databricks_conn_id_var)
    return {
        "DATABRICKS_HOST": fields["host"],
        "DATABRICKS_HTTP_PATH": fields["http_path"],
        "DATABRICKS_CLIENT_ID": fields["client_id"],
        "DATABRICKS_CLIENT_SECRET": fields["client_secret"],
    }


def open_connection(databricks_conn_id_var: str = "databricks_conn_id"):
    """Open a warehouse connection from the deployment's Databricks connection.

    `use_cloud_fetch=False` to match every other caller in the repo: this
    connection only ever runs scalar COUNT/EXISTS and small DISTINCT queries,
    and CloudFetch would route those through pre-signed S3 URLs — a pointless
    round-trip at best, and a failure where the warehouse or VPC does not
    allow it.
    """
    return get_databricks_connection(
        **databricks_conn_fields(databricks_conn_id_var),
        use_cloud_fetch=False,
    )


def _scalar(cursor, sql: str) -> int:
    """Run `sql` and return its single numeric result (0 when no row)."""
    cursor.execute(sql)
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _sql_string_literal_safe(value: str) -> str:
    """Escape a value for use inside a single-quoted SQL string literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def table_exists(conn, catalog: str, schema: str, table: str) -> bool:
    """True if the table is present in the catalog."""
    schema_safe = _sql_string_literal_safe(schema)
    table_safe = _sql_string_literal_safe(table)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT 1 FROM {_ident(catalog)}.information_schema.tables "
            f"WHERE table_schema = '{schema_safe}' AND table_name = '{table_safe}'"
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def list_tables(conn, catalog: str, schema: str) -> list[str]:
    """Every table name in the schema."""
    schema_safe = _sql_string_literal_safe(schema)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT table_name FROM {_ident(catalog)}.information_schema.tables "
            f"WHERE table_schema = '{schema_safe}'"
        )
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def run_gate(
    conn,
    catalog: str,
    schema: str,
    live_table: str,
    dated_table: str,
    gate: TableGate,
) -> None:
    """Run every check for one table. Raises ValueError on the first failure."""
    dated = fqn(catalog, schema, dated_table)
    live = fqn(catalog, schema, live_table)
    live_present = table_exists(conn, catalog, schema, live_table)

    cursor = conn.cursor()
    try:
        loaded = _scalar(cursor, count_sql(dated))

        distinct = loaded
        if gate.id_column is not None:
            distinct = _scalar(cursor, distinct_count_sql(dated, gate.id_column))
        check_distinct_ids(loaded, distinct, gate, dated_table)

        if gate.not_null_columns:
            check_nulls(_scalar(cursor, null_probe_sql(dated, gate.not_null_columns)), gate, dated_table)

        if gate.expected_sources:
            cursor.execute(distinct_sources_sql(dated, gate.source_column))
            found = {row[0] for row in cursor.fetchall() if row[0] is not None}
            check_sources(found, gate, dated_table)

        prior = _scalar(cursor, count_sql(live)) if live_present else 0
        check_counts(loaded, prior, gate, dated_table)

        if live_present and gate.id_column is not None and gate.min_id_overlap is not None:
            overlap = _scalar(cursor, overlap_sql(dated, live, gate.id_column))
            check_id_overlap(overlap, prior, gate, dated_table)
    finally:
        cursor.close()


def swap_table(conn, catalog: str, schema: str, live_table: str, dated_table: str) -> None:
    """Promote the dated vintage into the live name.

    Guards on the DATED table's existence, not just the live one's. The
    `swap` task loops cluster then pairwise with retries: if the cluster
    swap completes and the pairwise swap then raises, a naive retry that
    only checks the live table would see it present and re-run drop+rename —
    destroying `_old` (the backup) and then failing the final rename because
    the dated table it's looking for was already consumed by attempt one.
    Checking the dated table first makes each table's swap resumable: once
    it's gone, there is nothing left to do.
    """
    dated_present = table_exists(conn, catalog, schema, dated_table)
    live_present = table_exists(conn, catalog, schema, live_table)
    if not dated_present:
        if live_present:
            logger.info("%s already promoted; %s is gone. Nothing to do.", live_table, dated_table)
            return
        raise ValueError(f"Neither {dated_table} nor {live_table} exists — refusing to swap")

    cursor = conn.cursor()
    try:
        for statement in swap_statements(catalog, schema, live_table, dated_table, live_present):
            logger.info("Swap: %s", statement)
            execute_with_retry(cursor, statement)
    finally:
        cursor.close()


def drop_old_table(conn, catalog: str, schema: str, live_table: str) -> None:
    """Drop the renamed-aside table left by a completed swap."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {fqn(catalog, schema, old_name(live_table))}")
    finally:
        cursor.close()


def drop_stale_vintages(conn, catalog: str, schema: str, table: str, cutoff: str) -> list[str]:
    """Drop dated vintages older than `cutoff`. Returns what was dropped."""
    stale = stale_vintages(list_tables(conn, catalog, schema), table, cutoff)
    cursor = conn.cursor()
    try:
        for name in stale:
            logger.info("Dropping stale vintage %s", name)
            cursor.execute(f"DROP TABLE IF EXISTS {fqn(catalog, schema, name)}")
    finally:
        cursor.close()
    return stale
