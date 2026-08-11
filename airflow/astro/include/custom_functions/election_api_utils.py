"""Shared build-and-swap lifecycle for syncing Databricks marts into the
election-api Postgres database.

Each pipeline:

1. Drops and recreates `staging."<Target>_new"` with the same column shape as
   the live `public."<Target>"` table (no indexes — fast bulk-insert).
2. Streams the source mart from Databricks into the staging table.
3. Adds the PK, indexes, and FK constraints (canonical Prisma names suffixed
   with `_new`), all generated from the spec's declarative index/FK defs.
4. Runs quality checks against the staging table (generic count/column-contract
   gates plus optional per-table extras).
5. Atomic rename swap: `public."<Target>"` → `public."<Target>_old"`,
   `staging."<Target>_new"` → `public."<Target>"`. All indexes/constraints
   are renamed to/from canonical Prisma names so the post-swap shape matches
   the migration exactly. Any `<Target>_old` leftover from a run that crashed
   before step 6 is dropped first, inside the same transaction, so a crashed
   run never wedges subsequent ones.
6. Drops `public."<Target>_old"`.

Index/constraint names follow Prisma's convention (`<Table>_<col>_idx`,
`<Table>_pkey`, `<Table>_<col>_fkey`). Staging and archive variants insert
`_new` / `_old` after the table prefix so the same canonical name maps
predictably across all three schemas.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

import numpy as np
import psycopg2.extras
from include.custom_functions.databricks_utils import (
    read_databricks_partitioned,
    read_databricks_table,
)

logger = logging.getLogger("airflow.task")


@dataclass(frozen=True)
class Index:
    """Secondary index on the synced table: canonical Prisma name plus the
    parenthesized column expression (e.g. '(zip_code, position_id)')."""

    name: str
    expr: str
    unique: bool = False


@dataclass(frozen=True)
class ForeignKey:
    """Outbound FK from the synced table. A self-reference (ref_table equal to
    the synced table, e.g. Place.parent_id) is created against the staging
    table itself so it follows the table through the swap renames."""

    name: str
    column: str
    ref_table: str
    on_delete: str = "SET NULL"


@dataclass(frozen=True)
class InboundForeignKey:
    """A FK on a CHILD table referencing the swapped table.

    Constraints follow the referenced table's identity through renames, so
    after a rename-swap the child FK points at `<Target>_old` and drop_old
    fails on the dependency. The swap transaction therefore removes orphaned
    child rows (SET NULL when the FK's own ON DELETE is SET NULL, DELETE
    otherwise — a RESTRICT/CASCADE child cannot hold a NULL reference), drops
    this constraint, renames, and re-adds it against the fresh table,
    atomically.
    """

    child_table: str
    constraint_name: str
    child_column: str
    child_schema: str = "public"
    parent_column: str = "id"
    on_clause: str = "ON UPDATE CASCADE ON DELETE SET NULL"
    # Budget for child rows the swap may orphan: ~2x the historical daily
    # aging rate for Candidacy on Race.
    orphan_budget_ratio: float = 0.02


@dataclass(frozen=True)
class TableSyncSpec:
    """Declares a Databricks-mart-to-Postgres-table sync target."""

    target_table: str
    target_schema: str = "public"
    staging_schema: str = "staging"
    # Primary-key column; also used by the staged-vs-live id-overlap gate.
    pk_column: str = "id"
    indexes: tuple[Index, ...] = field(default_factory=tuple)
    fkeys: tuple[ForeignKey, ...] = field(default_factory=tuple)
    # FKs on OTHER tables referencing this one; re-pointed inside the swap.
    inbound_fkeys: tuple[InboundForeignKey, ...] = field(default_factory=tuple)

    @property
    def new_table(self) -> str:
        """Staging table name (`<Target>_new`)."""
        return f"{self.target_table}_new"

    @property
    def old_table(self) -> str:
        """Renamed-aside name during swap (`<Target>_old`)."""
        return f"{self.target_table}_old"

    @property
    def pk_name(self) -> str:
        """Canonical PK constraint name (`<Target>_pkey`)."""
        return f"{self.target_table}_pkey"

    @property
    def index_names(self) -> tuple[str, ...]:
        return tuple(idx.name for idx in self.indexes)

    @property
    def fkey_names(self) -> tuple[str, ...]:
        return tuple(fk.name for fk in self.fkeys)

    def stage_name(self, canonical: str) -> str:
        """`ZipToPosition_zip_code_idx` -> `ZipToPosition_new_zip_code_idx`."""
        return canonical.replace(self.target_table, self.new_table, 1)

    def archive_name(self, canonical: str) -> str:
        """`ZipToPosition_zip_code_idx` -> `ZipToPosition_old_zip_code_idx`."""
        return canonical.replace(self.target_table, self.old_table, 1)

    def constraint_ddl(self) -> list[str]:
        """PK + index + FK DDL for the staging table, stage-named."""
        sn, nt = self.staging_schema, self.new_table
        statements = [
            f'ALTER TABLE "{sn}"."{nt}" '
            f'ADD CONSTRAINT "{self.stage_name(self.pk_name)}" '
            f'PRIMARY KEY ("{self.pk_column}")'
        ]
        for idx in self.indexes:
            unique = "UNIQUE " if idx.unique else ""
            statements.append(
                f'CREATE {unique}INDEX "{self.stage_name(idx.name)}" ' f'ON "{sn}"."{nt}" {idx.expr}'
            )
        for fk in self.fkeys:
            if fk.ref_table == self.target_table:
                ref = f'"{sn}"."{nt}"'
            else:
                ref = f'"{self.target_schema}"."{fk.ref_table}"'
            statements.append(
                f'ALTER TABLE "{sn}"."{nt}" '
                f'ADD CONSTRAINT "{self.stage_name(fk.name)}" '
                f'FOREIGN KEY ("{fk.column}") REFERENCES {ref}(id) '
                f"ON UPDATE CASCADE ON DELETE {fk.on_delete}"
            )
        return statements


@dataclass(frozen=True)
class QualityGate:
    """Generic pre-swap gate config shared by every table group."""

    # Minimum plausible row count when no prior live table exists.
    cold_start_floor: int
    # Refuse the swap when loaded/prior falls below this ratio.
    min_prior_ratio: float = 0.5
    # Live columns the loader deliberately does not supply (Prisma-owned
    # columns whose defaults carry them, e.g. Person.is_pledged).
    db_owned_columns: frozenset[str] = frozenset()
    # Belt-and-braces NULL probes over the staged rows.
    not_null_columns: tuple[str, ...] = ()


def check_counts(loaded_count: int, prior_count: int, gate: QualityGate, table: str) -> None:
    """Pure count gate: ratio floor vs prior live, cold-start floor without."""
    if prior_count > 0:
        ratio = loaded_count / prior_count
        if ratio < gate.min_prior_ratio:
            raise ValueError(
                f"{table}: loaded {loaded_count} rows, prior live had "
                f"{prior_count} (ratio {ratio:.2f}) — refusing to swap"
            )
    elif loaded_count < gate.cold_start_floor:
        raise ValueError(
            f"{table}: cold-start load of {loaded_count} rows "
            f"(<{gate.cold_start_floor}) is implausibly small — refusing to swap"
        )


def check_column_contract(
    live_columns: set[str], loader_columns: set[str], gate: QualityGate, table: str
) -> None:
    """Pure schema-contract gate: fails closed if live grew a column the
    loader does not supply (a Prisma migration landing ahead of the loader —
    a swap would reset it wholesale), or if the loader supplies a column live
    lacks (drift; the load would already have failed, this is the proof)."""
    unknown = live_columns - loader_columns - gate.db_owned_columns
    missing = loader_columns - live_columns
    if unknown:
        raise ValueError(
            f"live {table} has columns the loader does not supply: "
            f"{sorted(unknown)}; a swap would reset them. Extend the column "
            f"list or allowlist in db_owned_columns"
        )
    if missing:
        raise ValueError(
            f"loader supplies columns live {table} lacks: {sorted(missing)}; "
            f"schema drift, refusing to swap"
        )


def run_quality_checks(
    conn,
    spec: TableSyncSpec,
    gate: QualityGate,
    loaded_count: int,
    loader_columns: Sequence[str],
) -> None:
    """Gather gate inputs from Postgres and apply the pure checks."""
    cur = conn.cursor()
    try:
        prior_exists, prior_count = prior_live_state(cur, spec)
        check_counts(loaded_count, prior_count if prior_exists else 0, gate, spec.target_table)

        if prior_exists:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s",
                (spec.target_schema, spec.target_table),
            )
            live_columns = {r[0] for r in cur.fetchall()}
            check_column_contract(live_columns, set(loader_columns), gate, spec.target_table)

        if gate.not_null_columns:
            predicate = " OR ".join(f'"{c}" IS NULL' for c in gate.not_null_columns)
            cur.execute(
                f'SELECT count(*) FROM "{spec.staging_schema}"."{spec.new_table}" ' f"WHERE {predicate}"
            )
            null_rows = cur.fetchone()[0]
            if null_rows > 0:
                raise ValueError(
                    f"{spec.target_table}: {null_rows} staging rows have a NULL "
                    f"in {list(gate.not_null_columns)} — refusing to swap"
                )
        logger.info(
            "Quality checks passed for %s: %d rows (prior %d)",
            spec.target_table,
            loaded_count,
            prior_count,
        )
    finally:
        cur.close()


def create_staging_table(conn, spec: TableSyncSpec) -> None:
    """Drop and recreate `staging.<table>_new` matching `public.<table>` columns."""
    cur = conn.cursor()
    try:
        cur.execute(f'DROP TABLE IF EXISTS "{spec.staging_schema}"."{spec.new_table}"')
        cur.execute(
            f'CREATE TABLE "{spec.staging_schema}"."{spec.new_table}" '
            f'(LIKE "{spec.target_schema}"."{spec.target_table}" INCLUDING DEFAULTS)'
        )
        conn.commit()
        logger.info(
            "Created %s.%s LIKE %s.%s",
            spec.staging_schema,
            spec.new_table,
            spec.target_schema,
            spec.target_table,
        )
    finally:
        cur.close()


def _pg_adaptable(value: object) -> object:
    """psycopg2 adapts native Python values, not numpy ones. The arrow-backed
    connector returns ARRAY columns as numpy arrays: typed when dense,
    object-dtype (numpy scalars and None inside) when mixed; whole-NULL arrays
    arrive as None and pass through untouched. A null-bearing integer array
    that arrow promotes to float64 still fails loudly at insert (int column,
    float values) rather than loading corrupted values."""
    if isinstance(value, np.ndarray):
        return [_pg_adaptable(x) for x in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def bulk_insert_from_databricks(
    conn,
    spec: TableSyncSpec,
    source_query: str,
    target_columns: Sequence[str],
    transform_row: Callable[[tuple], tuple] | None = None,
    batch_size: int = 5000,
    partition_column: str | None = None,
) -> int:
    """Stream `source_query` from Databricks into `staging.<table>_new` in batches.

    `transform_row` runs once per source row; if provided, the returned tuple
    must align with `target_columns`. If absent, source rows pass through
    unchanged (must already match `target_columns`).

    `partition_column`, if set, reads one distinct value at a time over a single
    Databricks connection (see `read_databricks_partitioned`), so peak worker
    memory stays bounded to a single partition instead of buffering the whole
    table — and without the per-partition connection churn that otherwise
    accumulates and OOMs the worker. The single commit still lands after the
    whole load, so a mid-load failure rolls it back and a retry starts from a
    clean `<table>_new`.
    """
    col_list = ", ".join(f'"{c}"' for c in target_columns)
    insert_sql = f'INSERT INTO "{spec.staging_schema}"."{spec.new_table}" ' f"({col_list}) VALUES %s"

    if partition_column is None:
        _col_names, batches = read_databricks_table(source_query, batch_size=batch_size)
    else:
        batches = read_databricks_partitioned(source_query, partition_column, batch_size=batch_size)

    total = 0
    cur = conn.cursor()
    try:
        for batch in batches:
            # Normalize numpy values (ARRAY columns, and any scalars inside
            # them) to native Python so psycopg2 can adapt them.
            rows = [
                tuple(_pg_adaptable(v) for v in (transform_row(r) if transform_row else r)) for r in batch
            ]
            if not rows:
                continue
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=batch_size)
            total += len(rows)
            if total % 50_000 < batch_size:
                logger.info("Inserted %d rows so far", total)
        conn.commit()
    except Exception:
        # Discard partial inserts so a retry starts from a clean <table>_new
        # (and the connection isn't left in an aborted-transaction state).
        conn.rollback()
        raise
    finally:
        cur.close()
        batches.close()

    logger.info("Loaded %d rows into %s.%s", total, spec.staging_schema, spec.new_table)
    return total


def apply_ddl(conn, statements: Sequence[str]) -> None:
    """Run a list of DDL statements in a single transaction."""
    cur = conn.cursor()
    try:
        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def prior_live_state(cur, spec: TableSyncSpec) -> tuple[bool, int]:
    """to_regclass existence + row count of the live target (identifiers
    double-quoted so mixed case survives).

    Both identifiers must be double-quoted in the regclass argument:
    Postgres folds unquoted mixed-case to lowercase, so `public.Race` would
    resolve to `public.race` and always return NULL.
    """
    cur.execute(
        "SELECT to_regclass(%s)",
        (f'"{spec.target_schema}"."{spec.target_table}"',),
    )
    exists = cur.fetchone()[0] is not None
    count = 0
    if exists:
        cur.execute(f'SELECT COUNT(*) FROM "{spec.target_schema}"."{spec.target_table}"')
        count = cur.fetchone()[0]
    return exists, count


def _check_inbound_gates(cur, spec: TableSyncSpec, id_overlap_floor: float) -> None:
    """Destructive-mutation gates, evaluated inside the swap transaction."""
    t = f'"{spec.target_schema}"."{spec.target_table}"'
    s = f'"{spec.staging_schema}"."{spec.new_table}"'
    cur.execute(
        f'SELECT count(*), count(stg."{spec.pk_column}") FROM {t} live '
        f'LEFT JOIN {s} stg ON live."{spec.pk_column}" = stg."{spec.pk_column}"'
    )
    live_count, overlap = cur.fetchone()
    if live_count > 0 and overlap / live_count < id_overlap_floor:
        raise ValueError(
            f"staged id overlap {overlap}/{live_count} below floor "
            f"{id_overlap_floor}; wholesale re-key suspected, refusing to swap"
        )
    for fk in spec.inbound_fkeys:
        cur.execute(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = %s::regclass AND conname = %s",
            (f'"{fk.child_schema}"."{fk.child_table}"', fk.constraint_name),
        )
        row = cur.fetchone()
        # Absent constraint = cold start or prior-crash state; nothing to
        # preserve. Present but with different actions = a migration changed
        # it; re-adding from our spec would silently revert it.
        if row is not None and not row[0].endswith(fk.on_clause):
            raise ValueError(
                f"live {fk.constraint_name} definition ({row[0]}) does not end "
                f"with the spec's on_clause ({fk.on_clause}); a migration may "
                f"have changed it; refusing to swap"
            )


def _check_orphan_budgets(cur, spec: TableSyncSpec) -> None:
    """Orphan budget per inbound FK: child rows whose reference is absent
    from staging. Joins only child against staging, so it guards cold starts
    too; the id-overlap floor (which needs the live table) does not."""
    s = f'"{spec.staging_schema}"."{spec.new_table}"'
    for fk in spec.inbound_fkeys:
        c = f'"{fk.child_schema}"."{fk.child_table}"'
        cur.execute(
            f'SELECT count(*) FILTER (WHERE c."{fk.child_column}" IS NOT NULL), '
            f'count(*) FILTER (WHERE c."{fk.child_column}" IS NOT NULL '
            f'AND stg."{fk.parent_column}" IS NULL) '
            f'FROM {c} c LEFT JOIN {s} stg ON stg."{fk.parent_column}" = c."{fk.child_column}"'
        )
        referencing, orphans = cur.fetchone()
        if referencing > 0 and orphans / referencing > fk.orphan_budget_ratio:
            raise ValueError(
                f"{orphans}/{referencing} {fk.child_table}.{fk.child_column} rows would "
                f"orphan (budget {fk.orphan_budget_ratio}); refusing to swap"
            )


def swap_staging_into_target(
    conn,
    spec: TableSyncSpec,
    *,
    lock_timeout: str = "10s",
    statement_timeout: str = "120s",
    # Floor on staged-vs-live id overlap: catches a wholesale re-key while
    # tolerating daily churn and the un-pruned interim sliver.
    id_overlap_floor: float = 0.95,
) -> None:
    """Atomic rename-swap: stage `_new` -> `public.<table>`, old -> `_old`.

    With `spec.inbound_fkeys`, the same transaction also re-points child FKs:
    bounded lock/statement timeouts, child-first lock order (matches the
    legacy writer's DML order, so an overlapping writer run cannot AB-BA
    deadlock), in-transaction destructive gates (staged-vs-live id-overlap
    floor, gated on the live target existing, and an orphaned-child budget
    that runs on both branches so it guards cold starts too, evaluated under
    the locks, where they must be current at the moment of mutation; the
    task-level quality gates cover counts, window, and schema shape earlier
    and more cheaply), orphan removal mirroring the FK's own ON DELETE action
    (SET NULL nulls the reference; RESTRICT/CASCADE children are deleted —
    they cannot hold a NULL reference and are themselves swap-refreshed),
    constraint drop, renames, re-add validated. A crash anywhere rolls the
    whole transaction back, including the orphan removal and the constraint
    drop.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
            (spec.target_schema, spec.target_table),
        )
        target_exists = cur.fetchone() is not None

        if spec.inbound_fkeys:
            cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
            cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
            for inbound in spec.inbound_fkeys:
                cur.execute(
                    f'LOCK TABLE "{inbound.child_schema}"."{inbound.child_table}" '
                    f"IN ACCESS EXCLUSIVE MODE"
                )
            if target_exists:
                cur.execute(
                    f'LOCK TABLE "{spec.target_schema}"."{spec.target_table}" ' f"IN ACCESS EXCLUSIVE MODE"
                )
                _check_inbound_gates(cur, spec, id_overlap_floor)
            _check_orphan_budgets(cur, spec)
            for inbound in spec.inbound_fkeys:
                not_in_staging = (
                    f'c."{inbound.child_column}" IS NOT NULL AND NOT EXISTS '
                    f'(SELECT 1 FROM "{spec.staging_schema}"."{spec.new_table}" stg '
                    f'WHERE stg."{inbound.parent_column}" = c."{inbound.child_column}")'
                )
                if inbound.on_clause.endswith("SET NULL"):
                    cur.execute(
                        f'UPDATE "{inbound.child_schema}"."{inbound.child_table}" AS c '
                        f'SET "{inbound.child_column}" = NULL '
                        f"WHERE {not_in_staging}"
                    )
                else:
                    cur.execute(
                        f'DELETE FROM "{inbound.child_schema}"."{inbound.child_table}" AS c '
                        f"WHERE {not_in_staging}"
                    )
                cur.execute(
                    f'ALTER TABLE "{inbound.child_schema}"."{inbound.child_table}" '
                    f'DROP CONSTRAINT IF EXISTS "{inbound.constraint_name}"'
                )

        statements: list[str] = []
        # A `<table>_old` left behind by a run that died in the swap->drop_old
        # window would collide with every rename below (table name and the
        # `_old`-suffixed index/constraint names), failing each subsequent run
        # until someone dropped it by hand — and five consecutive failed runs
        # auto-pause the DAG. Pre-drop it in this transaction: DDL is
        # transactional, so a mid-swap failure rolls the drop back with the
        # renames, and a clean run drops nothing that drop_old_table wouldn't.
        statements.append(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}"')
        if target_exists:
            statements.append(
                f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" ' f'RENAME TO "{spec.old_table}"'
            )
            statements.append(
                f'ALTER INDEX "{spec.target_schema}"."{spec.pk_name}" '
                f'RENAME TO "{spec.archive_name(spec.pk_name)}"'
            )
            for idx in spec.index_names:
                statements.append(
                    f'ALTER INDEX "{spec.target_schema}"."{idx}" ' f'RENAME TO "{spec.archive_name(idx)}"'
                )
            for fk in spec.fkey_names:
                statements.append(
                    f'ALTER TABLE "{spec.target_schema}"."{spec.old_table}" '
                    f'RENAME CONSTRAINT "{fk}" '
                    f'TO "{spec.archive_name(fk)}"'
                )

        statements.append(
            f'ALTER TABLE "{spec.staging_schema}"."{spec.new_table}" ' f'SET SCHEMA "{spec.target_schema}"'
        )
        statements.append(
            f'ALTER TABLE "{spec.target_schema}"."{spec.new_table}" ' f'RENAME TO "{spec.target_table}"'
        )
        statements.append(
            f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(spec.pk_name)}" '
            f'RENAME TO "{spec.pk_name}"'
        )
        for idx in spec.index_names:
            statements.append(
                f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(idx)}" ' f'RENAME TO "{idx}"'
            )
        for fk in spec.fkey_names:
            statements.append(
                f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" '
                f'RENAME CONSTRAINT "{spec.stage_name(fk)}" '
                f'TO "{fk}"'
            )

        for stmt in statements:
            cur.execute(stmt)

        for inbound in spec.inbound_fkeys:
            cur.execute(
                f'ALTER TABLE "{inbound.child_schema}"."{inbound.child_table}" '
                f'ADD CONSTRAINT "{inbound.constraint_name}" '
                f'FOREIGN KEY ("{inbound.child_column}") '
                f'REFERENCES "{spec.target_schema}"."{spec.target_table}"'
                f'("{inbound.parent_column}") {inbound.on_clause}'
            )
        conn.commit()
        logger.info(
            "Swap complete for %s (target_existed=%s, inbound_fkeys=%d)",
            spec.target_table,
            target_exists,
            len(spec.inbound_fkeys),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def drop_old_table(conn, spec: TableSyncSpec) -> None:
    """DROP TABLE IF EXISTS `public.<table>_old`. Run after swap commits."""
    cur = conn.cursor()
    try:
        cur.execute(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}"')
        conn.commit()
        logger.info("Dropped %s.%s if it existed", spec.target_schema, spec.old_table)
    finally:
        cur.close()
