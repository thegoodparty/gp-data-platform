"""Shared build-and-swap lifecycle for syncing Databricks marts into the
election-api Postgres database.

Each pipeline:

1. Drops and recreates `staging."<Target>_new"` with the same column shape as
   the live `public."<Target>"` table (no indexes — fast bulk-insert).
2. Streams the source mart from Databricks into the staging table.
3. Adds the PK, indexes, and FK constraints (canonical Prisma names suffixed
   with `_new`).
4. Runs caller-supplied quality checks against the staging table.
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

import psycopg2.extras
from include.custom_functions.databricks_utils import (
    read_databricks_partitioned,
    read_databricks_table,
)

logger = logging.getLogger("airflow.task")


@dataclass(frozen=True)
class InboundForeignKey:
    """A FK on a CHILD table referencing the swapped table.

    Constraints follow the referenced table's identity through renames, so
    after a rename-swap the child FK points at `<Target>_old` and drop_old
    fails on the dependency. The swap transaction therefore nulls orphaned
    child rows, drops this constraint, renames, and re-adds it against the
    fresh table, atomically.
    """

    child_table: str
    constraint_name: str
    child_column: str
    child_schema: str = "public"
    parent_column: str = "id"
    on_clause: str = "ON UPDATE CASCADE ON DELETE SET NULL"


@dataclass(frozen=True)
class TableSyncSpec:
    """Declares a Databricks-mart-to-Postgres-table sync target."""

    target_table: str
    target_schema: str = "public"
    staging_schema: str = "staging"
    pk_constraint_name: str | None = None
    # Canonical (post-swap) index names — used to rename during the swap.
    # Do NOT include the PK constraint here; it's tracked separately.
    indexes: tuple[str, ...] = field(default_factory=tuple)
    # Canonical (post-swap) foreign-key constraint names.
    fkeys: tuple[str, ...] = field(default_factory=tuple)
    # FKs on OTHER tables referencing this one; re-pointed inside the swap.
    inbound_fkeys: tuple[InboundForeignKey, ...] = field(default_factory=tuple)
    # Primary-key column used by the staged-vs-live id-overlap gate.
    pk_column: str = "id"

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
        """Canonical PK constraint name (defaults to `<Target>_pkey`)."""
        return self.pk_constraint_name or f"{self.target_table}_pkey"

    def stage_name(self, canonical: str) -> str:
        """`ZipToPosition_zip_code_idx` -> `ZipToPosition_new_zip_code_idx`."""
        return canonical.replace(self.target_table, self.new_table, 1)

    def archive_name(self, canonical: str) -> str:
        """`ZipToPosition_zip_code_idx` -> `ZipToPosition_old_zip_code_idx`."""
        return canonical.replace(self.target_table, self.old_table, 1)


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
            rows = [transform_row(r) if transform_row else tuple(r) for r in batch]
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


def _check_inbound_gates(
    cur, spec: TableSyncSpec, id_overlap_floor: float, orphan_budget_ratio: float
) -> None:
    """Destructive-mutation gates, evaluated inside the swap transaction."""
    t = f'"{spec.target_schema}"."{spec.target_table}"'
    s = f'"{spec.staging_schema}"."{spec.new_table}"'
    cur.execute(f"SELECT count(*) FROM {t}")
    live_count = cur.fetchone()[0]
    cur.execute(
        f'SELECT count(*) FROM {t} live JOIN {s} stg ON live."{spec.pk_column}" = stg."{spec.pk_column}"'
    )
    overlap = cur.fetchone()[0]
    if live_count > 0 and overlap / live_count < id_overlap_floor:
        raise ValueError(
            f"staged id overlap {overlap}/{live_count} below floor "
            f"{id_overlap_floor}; wholesale re-key suspected, refusing to swap"
        )
    for fk in spec.inbound_fkeys:
        c = f'"{fk.child_schema}"."{fk.child_table}"'
        cur.execute(f'SELECT count(*) FROM {c} c WHERE c."{fk.child_column}" IS NOT NULL')
        referencing = cur.fetchone()[0]
        cur.execute(
            f'SELECT count(*) FROM {c} c WHERE c."{fk.child_column}" IS NOT NULL '
            f"AND NOT EXISTS (SELECT 1 FROM {s} stg "
            f'WHERE stg."{fk.parent_column}" = c."{fk.child_column}")'
        )
        orphans = cur.fetchone()[0]
        if referencing > 0 and orphans / referencing > orphan_budget_ratio:
            raise ValueError(
                f"{orphans}/{referencing} {fk.child_table}.{fk.child_column} rows would "
                f"orphan (budget {orphan_budget_ratio}); refusing to swap"
            )


def swap_staging_into_target(
    conn,
    spec: TableSyncSpec,
    *,
    lock_timeout: str = "10s",
    statement_timeout: str = "120s",
    id_overlap_floor: float = 0.95,
    orphan_budget_ratio: float = 0.02,
) -> None:
    """Atomic rename-swap: stage `_new` -> `public.<table>`, old -> `_old`.

    With `spec.inbound_fkeys`, the same transaction also re-points child FKs:
    bounded lock/statement timeouts, child-first lock order (matches the
    legacy writer's DML order, so an overlapping writer run cannot AB-BA
    deadlock), in-transaction destructive gates (staged-vs-live id-overlap
    floor and an orphaned-child budget, recomputed under the locks because
    task-level checks are stale by swap time), orphan SET NULL mirroring the
    FK's own ON DELETE action, constraint drop, renames, re-add validated.
    A crash anywhere rolls the whole transaction back, including the orphan
    nulls and the constraint drop.
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
            for fk in spec.inbound_fkeys:
                cur.execute(f'LOCK TABLE "{fk.child_schema}"."{fk.child_table}" ' f"IN ACCESS EXCLUSIVE MODE")
            if target_exists:
                cur.execute(
                    f'LOCK TABLE "{spec.target_schema}"."{spec.target_table}" ' f"IN ACCESS EXCLUSIVE MODE"
                )
                _check_inbound_gates(cur, spec, id_overlap_floor, orphan_budget_ratio)
            for fk in spec.inbound_fkeys:
                cur.execute(
                    f'UPDATE "{fk.child_schema}"."{fk.child_table}" AS c '
                    f'SET "{fk.child_column}" = NULL '
                    f'WHERE c."{fk.child_column}" IS NOT NULL AND NOT EXISTS '
                    f'(SELECT 1 FROM "{spec.staging_schema}"."{spec.new_table}" stg '
                    f'WHERE stg."{fk.parent_column}" = c."{fk.child_column}")'
                )
                cur.execute(
                    f'ALTER TABLE "{fk.child_schema}"."{fk.child_table}" '
                    f'DROP CONSTRAINT IF EXISTS "{fk.constraint_name}"'
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
            for idx in spec.indexes:
                statements.append(
                    f'ALTER INDEX "{spec.target_schema}"."{idx}" ' f'RENAME TO "{spec.archive_name(idx)}"'
                )
            for fk in spec.fkeys:
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
        for idx in spec.indexes:
            statements.append(
                f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(idx)}" ' f'RENAME TO "{idx}"'
            )
        for fk in spec.fkeys:
            statements.append(
                f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" '
                f'RENAME CONSTRAINT "{spec.stage_name(fk)}" '
                f'TO "{fk}"'
            )

        for stmt in statements:
            cur.execute(stmt)

        for fk in spec.inbound_fkeys:
            cur.execute(
                f'ALTER TABLE "{fk.child_schema}"."{fk.child_table}" '
                f'ADD CONSTRAINT "{fk.constraint_name}" '
                f'FOREIGN KEY ("{fk.child_column}") '
                f'REFERENCES "{spec.target_schema}"."{spec.target_table}"'
                f'("{fk.parent_column}") {fk.on_clause}'
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
