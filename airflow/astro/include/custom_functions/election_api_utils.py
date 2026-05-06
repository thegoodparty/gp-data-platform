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
   the migration exactly.
6. Drops `public."<Target>_old"`.

Index/constraint names follow Prisma's convention (`<Table>_<col>_idx`,
`<Table>_pkey`, `<Table>_<col>_fkey`). Staging and archive variants insert
`_new` / `_old` after the table prefix so the same canonical name maps
predictably across all three schemas.
"""

import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple

import psycopg2.extras
from include.custom_functions.databricks_utils import read_databricks_table

logger = logging.getLogger("airflow.task")


@dataclass(frozen=True)
class TableSyncSpec:
    """Declares a Databricks-mart-to-Postgres-table sync target."""

    target_table: str
    target_schema: str = "public"
    staging_schema: str = "staging"
    pk_constraint_name: Optional[str] = None
    # Canonical (post-swap) index names — used to rename during the swap.
    # Do NOT include the PK constraint here; it's tracked separately.
    indexes: Tuple[str, ...] = field(default_factory=tuple)
    # Canonical (post-swap) foreign-key constraint names.
    fkeys: Tuple[str, ...] = field(default_factory=tuple)

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
    transform_row: Optional[Callable[[tuple], tuple]] = None,
    batch_size: int = 5000,
) -> int:
    """Stream `source_query` from Databricks into `staging.<table>_new` in batches.

    `transform_row` runs once per source row; if provided, the returned tuple
    must align with `target_columns`. If absent, source rows pass through
    unchanged (must already match `target_columns`).
    """
    logger.info("Reading from Databricks: %s", source_query)
    _col_names, batches = read_databricks_table(source_query, batch_size=batch_size)

    col_list = ", ".join(f'"{c}"' for c in target_columns)
    insert_sql = (
        f'INSERT INTO "{spec.staging_schema}"."{spec.new_table}" '
        f"({col_list}) VALUES %s"
    )

    total = 0
    try:
        cur = conn.cursor()
        try:
            for batch in batches:
                rows = [transform_row(r) if transform_row else tuple(r) for r in batch]
                if not rows:
                    continue
                psycopg2.extras.execute_values(
                    cur, insert_sql, rows, page_size=batch_size
                )
                total += len(rows)
                if total % 50_000 < batch_size:
                    logger.info("Inserted %d rows so far", total)
            conn.commit()
        finally:
            cur.close()
    finally:
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


def swap_staging_into_target(conn, spec: TableSyncSpec) -> None:
    """Atomic rename-swap: stage `_new` -> `public.<table>`, old -> `_old`.

    All indexes/constraints listed on `spec` are renamed to/from canonical
    Prisma names so the post-swap shape matches the migration exactly. Safe
    on first run when `public.<table>` doesn't yet exist (skip the rename-old
    branch).
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
            (spec.target_schema, spec.target_table),
        )
        target_exists = cur.fetchone() is not None

        statements: List[str] = []
        if target_exists:
            statements.append(
                f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" '
                f'RENAME TO "{spec.old_table}"'
            )
            statements.append(
                f'ALTER INDEX "{spec.target_schema}"."{spec.pk_name}" '
                f'RENAME TO "{spec.archive_name(spec.pk_name)}"'
            )
            for idx in spec.indexes:
                statements.append(
                    f'ALTER INDEX "{spec.target_schema}"."{idx}" '
                    f'RENAME TO "{spec.archive_name(idx)}"'
                )
            for fk in spec.fkeys:
                statements.append(
                    f'ALTER TABLE "{spec.target_schema}"."{spec.old_table}" '
                    f'RENAME CONSTRAINT "{fk}" '
                    f'TO "{spec.archive_name(fk)}"'
                )

        statements.append(
            f'ALTER TABLE "{spec.staging_schema}"."{spec.new_table}" '
            f'SET SCHEMA "{spec.target_schema}"'
        )
        statements.append(
            f'ALTER TABLE "{spec.target_schema}"."{spec.new_table}" '
            f'RENAME TO "{spec.target_table}"'
        )
        statements.append(
            f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(spec.pk_name)}" '
            f'RENAME TO "{spec.pk_name}"'
        )
        for idx in spec.indexes:
            statements.append(
                f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(idx)}" '
                f'RENAME TO "{idx}"'
            )
        for fk in spec.fkeys:
            statements.append(
                f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" '
                f'RENAME CONSTRAINT "{spec.stage_name(fk)}" '
                f'TO "{fk}"'
            )

        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
        logger.info(
            "Swap complete for %s (target_existed=%s)",
            spec.target_table,
            target_exists,
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
