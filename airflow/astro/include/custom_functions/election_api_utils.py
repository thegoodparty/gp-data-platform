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

Tables tied together by FKs (Person ← OfficeHolder) cannot swap
independently: an FK constraint binds to the referenced table's OID, so an FK
pointing at a swapped table would follow the renamed-aside `_old` copy. Those
tables stage their FKs staged-to-staged (`OfficeHolder_new.person_id` →
`staging."Person_new"`, populated in dependency order) and swap together via
`swap_group_staging_into_target` — the constraints ride the renames and come
out pointing at the fresh tables. Its single transaction also runs
caller-supplied DDL before the renames (drop inbound FKs held by tables
outside the group, e.g. Candidacy → Person) and after them (null orphans and
re-create those FKs against the fresh tables). Group `specs` are ordered
parent-first; leftover `_old` pre-drops run child-first so a dependent FK
never blocks its parent.

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
    # Canonical (post-swap) foreign-key constraint names. Only FKs that ride
    # the rename swap belong here (outbound FKs staged with `_new` names);
    # FKs re-created from scratch after a group swap do not.
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


def _archive_statements(spec: TableSyncSpec) -> list[str]:
    """Rename the live table (and its indexes/constraints) aside to `_old`."""
    statements = [
        f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" ' f'RENAME TO "{spec.old_table}"',
        f'ALTER INDEX "{spec.target_schema}"."{spec.pk_name}" '
        f'RENAME TO "{spec.archive_name(spec.pk_name)}"',
    ]
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
    return statements


def _promote_statements(spec: TableSyncSpec) -> list[str]:
    """Move `staging.<table>_new` into place under canonical names."""
    statements = [
        f'ALTER TABLE "{spec.staging_schema}"."{spec.new_table}" ' f'SET SCHEMA "{spec.target_schema}"',
        f'ALTER TABLE "{spec.target_schema}"."{spec.new_table}" ' f'RENAME TO "{spec.target_table}"',
        f'ALTER INDEX "{spec.target_schema}"."{spec.stage_name(spec.pk_name)}" '
        f'RENAME TO "{spec.pk_name}"',
    ]
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
    return statements


def swap_group_staging_into_target(
    conn,
    specs: Sequence[TableSyncSpec],
    pre_swap_ddl: Sequence[str] = (),
    post_swap_ddl: Sequence[str] = (),
) -> None:
    """Atomic rename-swap of one or more staged tables in a single transaction.

    `specs` must be ordered parent-first when the tables reference each other:
    leftover `_old` pre-drops run in reverse (child-first) so a dependent FK on
    a leftover child never blocks dropping its parent.

    `pre_swap_ddl` runs after the pre-drops and before any rename — the place
    to drop inbound FK constraints held by tables outside the group (they bind
    to the live table's OID and would otherwise follow it to `_old`).
    `post_swap_ddl` runs after every rename — the place to clean up orphans
    and re-create those FKs against the fresh tables. Both ride the same
    transaction, so any failure rolls the whole swap back.

    Safe on first run when a live table doesn't yet exist (its rename-old
    branch is skipped), and self-healing when a prior run crashed between this
    transaction committing and `drop_old_table` completing: the leftover
    `_old` tables are dropped inside the same transaction before the renames.
    """
    cur = conn.cursor()
    try:
        target_exists: dict[str, bool] = {}
        for spec in specs:
            cur.execute(
                "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
                (spec.target_schema, spec.target_table),
            )
            target_exists[spec.target_table] = cur.fetchone() is not None

        statements: list[str] = []
        # A `<table>_old` left behind by a run that died in the swap->drop_old
        # window would collide with every rename below (table name and the
        # `_old`-suffixed index/constraint names), failing each subsequent run
        # until someone dropped it by hand — and five consecutive failed runs
        # auto-pause the DAG. Pre-drop it in this transaction: DDL is
        # transactional, so a mid-swap failure rolls the drop back with the
        # renames, and a clean run drops nothing that drop_old_table wouldn't.
        for spec in reversed(specs):
            statements.append(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}"')
        statements.extend(pre_swap_ddl)
        for spec in specs:
            if target_exists[spec.target_table]:
                statements.extend(_archive_statements(spec))
        for spec in specs:
            statements.extend(_promote_statements(spec))
        statements.extend(post_swap_ddl)

        for stmt in statements:
            cur.execute(stmt)
        conn.commit()
        logger.info(
            "Swap complete for %s (target_existed=%s)",
            ", ".join(spec.target_table for spec in specs),
            target_exists,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def swap_staging_into_target(conn, spec: TableSyncSpec) -> None:
    """Atomic rename-swap: stage `_new` -> `public.<table>`, old -> `_old`.

    Single-table form of `swap_group_staging_into_target`.
    """
    swap_group_staging_into_target(conn, [spec])


def drop_old_table(conn, spec: TableSyncSpec) -> None:
    """DROP TABLE IF EXISTS `public.<table>_old`. Run after swap commits.

    For a group swap, call once per spec in child-first order so a dependent
    FK on a leftover child never blocks dropping its parent.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}"')
        conn.commit()
        logger.info("Dropped %s.%s if it existed", spec.target_schema, spec.old_table)
    finally:
        cur.close()
