"""Pre-swap quality gates and the dated-vintage swap for the matcha ER outputs.

The matcha container writes each run to a DATED table
(`er_source.clustered_candidacy_stages_20260825`) and never to a live one: its
upload is `CREATE OR REPLACE TABLE` followed by `COPY INTO`, so pointing it at
a live table means a mid-upload failure leaves every downstream dbt model
reading an empty or partial table. This module is the other half of that
contract — it gates the dated table, then renames it into the live name.

Unity Catalog has no multi-statement transaction, so the swap is an explicit
idempotent sequence rather than one atomic statement, and the leftover `_old`
from a crashed run is dropped first so a crash never wedges the next run.

Everything above the "Databricks execution" divider is pure, so the gate logic
is testable without a warehouse.
"""

import re
from dataclasses import dataclass


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
    explicit and idempotent on retry. The pre-drop MUST come first: a crash
    between the swap and cleanup leaves an `_old` table behind, and the next
    run's rename-aside would collide with it — failing run after run until a
    human intervened.
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
