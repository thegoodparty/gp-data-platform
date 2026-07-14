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


def _archive_live_statements(spec: TableSyncSpec) -> list[str]:
    """DDL to rename the live table (and its pk/indexes/fkeys) aside to `_old`.

    Only valid when `public.<table>` exists — callers guard on existence.
    """
    statements = [
        f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" RENAME TO "{spec.old_table}"',
        f'ALTER INDEX "{spec.target_schema}"."{spec.pk_name}" '
        f'RENAME TO "{spec.archive_name(spec.pk_name)}"',
    ]
    for idx in spec.indexes:
        statements.append(f'ALTER INDEX "{spec.target_schema}"."{idx}" RENAME TO "{spec.archive_name(idx)}"')
    for fk in spec.fkeys:
        statements.append(
            f'ALTER TABLE "{spec.target_schema}"."{spec.old_table}" '
            f'RENAME CONSTRAINT "{fk}" TO "{spec.archive_name(fk)}"'
        )
    return statements


def _promote_new_statements(spec: TableSyncSpec) -> list[str]:
    """DDL to move `staging.<table>_new` into the target schema and rename it
    (and its pk/indexes/fkeys) to the canonical Prisma names."""
    statements = [
        f'ALTER TABLE "{spec.staging_schema}"."{spec.new_table}" SET SCHEMA "{spec.target_schema}"',
        f'ALTER TABLE "{spec.target_schema}"."{spec.new_table}" RENAME TO "{spec.target_table}"',
        f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(spec.pk_name)}" '
        f'RENAME TO "{spec.pk_name}"',
    ]
    for idx in spec.indexes:
        statements.append(f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(idx)}" RENAME TO "{idx}"')
    for fk in spec.fkeys:
        statements.append(
            f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" '
            f'RENAME CONSTRAINT "{spec.stage_name(fk)}" TO "{fk}"'
        )
    return statements


def swap_graph_into_target(conn, specs: Sequence[TableSyncSpec]) -> None:
    """Atomic rename-swap of an entire interdependent table set in ONE txn.

    `specs` MUST be in dependency order (parents before children). Each spec's
    `_new` table must already carry FK constraints referencing its SIBLING
    `_new` tables in the staging schema (not the live parents), so after every
    table is promoted the FK OIDs resolve to the new live parents. Because the
    whole set is renamed in one transaction, readers see either the entire prior
    graph or the entire new graph, never a mix.

    Safe when a live target does not yet exist (that table skips its archive
    branch). Self-healing: a full `_old` set left by a crash in the
    swap->drop_old_graph window is pre-dropped here, children-first, inside the
    same transaction — a mid-swap failure rolls the pre-drop back with the
    renames, so a crashed run never wedges the next one.
    """
    specs = list(specs)
    cur = conn.cursor()
    try:
        exists: dict[str, bool] = {}
        for spec in specs:
            cur.execute(
                "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
                (spec.target_schema, spec.target_table),
            )
            exists[spec.target_table] = cur.fetchone() is not None

        statements: list[str] = []
        # 1. Pre-drop any leftover `_old`, CHILDREN FIRST (reverse dep order) so a
        #    child_old's archived FK never blocks dropping its parent_old.
        for spec in reversed(specs):
            statements.append(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}"')
        # 2. Archive each live table aside to `_old` (skip tables not yet live).
        for spec in specs:
            if exists[spec.target_table]:
                statements.extend(_archive_live_statements(spec))
        # 3. Promote every `_new` to its canonical name. Renames are OID-preserving,
        #    so sibling-referencing FKs follow their parent into the live schema.
        for spec in specs:
            statements.extend(_promote_new_statements(spec))

        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
        logger.info("Graph swap complete for %d tables", len(specs))
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def swap_staging_into_target(conn, spec: TableSyncSpec) -> None:
    """Atomic rename-swap of a single table. Thin wrapper over
    `swap_graph_into_target` so the one-table lifecycle and its crash-recovery
    guarantees are identical to the graph case."""
    swap_graph_into_target(conn, [spec])


def drop_old_graph(conn, specs: Sequence[TableSyncSpec]) -> None:
    """DROP the archived `_old` set, CHILDREN FIRST. Idempotent (`IF EXISTS`)."""
    specs = list(specs)
    cur = conn.cursor()
    try:
        for spec in reversed(specs):
            cur.execute(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}"')
        conn.commit()
        logger.info("Dropped `_old` for %d tables if they existed", len(specs))
    finally:
        cur.close()


def drop_old_table(conn, spec: TableSyncSpec) -> None:
    """DROP TABLE IF EXISTS `public.<table>_old`. Run after swap commits."""
    drop_old_graph(conn, [spec])
