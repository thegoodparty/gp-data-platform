"""Steps of the build-and-swap lifecycle that syncs Databricks marts into the
election-api Postgres database. The `sync_election_api` DAG docstring
describes the pipeline these compose into.

Index and constraint names follow Prisma's convention (`<Table>_<col>_idx`,
`<Table>_pkey`, `<Table>_<col>_fkey`). Staging and archive variants insert
`_new` / `_old` after the table prefix, so one canonical name maps
predictably across all three schemas.
"""

import logging
from collections.abc import Sequence
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
    """Outbound FK from the synced table. Created against the referenced
    table's STAGING name (self-references included), so it follows the
    staging set through the swap renames."""

    name: str
    column: str
    ref_table: str
    on_delete: str = "SET NULL"


@dataclass(frozen=True)
class TableSyncSpec:
    """Declares a Databricks-mart-to-Postgres-table sync target."""

    target_table: str
    target_schema: str = "public"
    staging_schema: str = "staging"
    pk_column: str = "id"
    indexes: tuple[Index, ...] = field(default_factory=tuple)
    fkeys: tuple[ForeignKey, ...] = field(default_factory=tuple)

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
        """PK + index + FK DDL for the staging table, stage-named.

        FKs reference the sibling staging table (`<Ref>_new`): the referenced
        parent must already be loaded with its PK in place, which the DAG
        enforces with a parent.build_indexes >> child.build_indexes edge.
        """
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
            statements.append(
                f'ALTER TABLE "{sn}"."{nt}" '
                f'ADD CONSTRAINT "{self.stage_name(fk.name)}" '
                f'FOREIGN KEY ("{fk.column}") REFERENCES "{sn}"."{fk.ref_table}_new"(id) '
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
    # Floor on staged-vs-live id overlap; catches a wholesale re-key of a
    # table whose ids external consumers may hold. None skips the check
    # (tables whose ids legitimately re-mint, e.g. model-version keyed).
    min_id_overlap: float | None = None
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


def check_id_overlap(overlap: int, prior_count: int, gate: QualityGate, table: str) -> None:
    """Pure id-overlap gate: too few shared ids means the staged set re-keyed
    wholesale rather than refreshed. No declared floor skips the check, since
    some tables' ids legitimately re-mint."""
    if gate.min_id_overlap is None or prior_count <= 0:
        return
    if overlap / prior_count < gate.min_id_overlap:
        raise ValueError(
            f"{table}: staged id overlap {overlap}/{prior_count} "
            f"below floor {gate.min_id_overlap}; wholesale re-key "
            f"suspected — refusing to swap"
        )


def check_nulls(null_rows: int, gate: QualityGate, table: str) -> None:
    """Pure NULL probe over the staged rows."""
    if null_rows > 0:
        raise ValueError(
            f"{table}: {null_rows} staging rows have a NULL "
            f"in {list(gate.not_null_columns)} — refusing to swap"
        )


def staging_columns(conn, spec: TableSyncSpec) -> list[str]:
    """Ordered column names of the staging clone (which mirrors the live
    table). The loader selects exactly these from the mart, so dbt must
    publish every one of them, with matching names and types."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (spec.staging_schema, spec.new_table),
        )
        columns = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
    if not columns:
        raise ValueError(f"no columns found for {spec.staging_schema}.{spec.new_table}")
    return columns


def run_quality_checks(
    conn,
    spec: TableSyncSpec,
    gate: QualityGate,
    loaded_count: int,
) -> None:
    """Gather gate inputs from Postgres and apply the pure checks."""
    cur = conn.cursor()
    try:
        # prior_count is 0 when the live table is absent, which check_counts
        # reads as a cold start.
        _, prior_count = prior_live_state(cur, spec)
        check_counts(loaded_count, prior_count, gate, spec.target_table)

        # Guard the query, not just the check: the join is expensive on the
        # large tables and pointless without a declared floor.
        if gate.min_id_overlap is not None and prior_count > 0:
            cur.execute(
                f'SELECT count(stg."{spec.pk_column}") '
                f'FROM "{spec.target_schema}"."{spec.target_table}" live '
                f'JOIN "{spec.staging_schema}"."{spec.new_table}" stg '
                f'ON live."{spec.pk_column}" = stg."{spec.pk_column}"'
            )
            check_id_overlap(cur.fetchone()[0], prior_count, gate, spec.target_table)

        if gate.not_null_columns:
            predicate = " OR ".join(f'"{c}" IS NULL' for c in gate.not_null_columns)
            cur.execute(
                f'SELECT count(*) FROM "{spec.staging_schema}"."{spec.new_table}" ' f"WHERE {predicate}"
            )
            check_nulls(cur.fetchone()[0], gate, spec.target_table)
        logger.info(
            "Quality checks passed for %s: %d rows (prior %d)",
            spec.target_table,
            loaded_count,
            prior_count,
        )
    finally:
        cur.close()


def create_staging_table(conn, spec: TableSyncSpec) -> None:
    """Drop and recreate `staging.<table>_new` matching `public.<table>` columns.

    CASCADE, because sibling staging tables may hold FKs to this one from a
    prior run. That drops the sibling's FK constraint, not the sibling; if the
    sibling's rebuild does not restore it, the swap fails closed on the missing
    constraint rename rather than shipping a table without its FK.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'DROP TABLE IF EXISTS "{spec.staging_schema}"."{spec.new_table}" CASCADE')
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
    """psycopg2 adapts native Python values, not numpy ones, and the arrow-backed
    connector returns ARRAY columns as numpy arrays (nested numpy scalars when
    mixed-dtype). Whole-NULL arrays arrive as None and pass through."""
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
    batch_size: int = 5000,
    partition_column: str | None = None,
) -> int:
    """Stream `source_query` from Databricks into `staging.<table>_new` in batches.

    Source rows pass through unchanged, so the query's column order must
    match `target_columns`.

    `batch_size` is what bounds peak worker memory: it is both the insert page
    size and the Databricks cursor's fetch size. `partition_column`, if set,
    additionally reads one distinct value at a time over a single Databricks
    connection (see `read_databricks_partitioned`), keeping each server-side
    result set small. The commit lands after the whole load, so a mid-load
    failure rolls back and a retry starts from a clean `<table>_new`.
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
            rows = [tuple(_pg_adaptable(v) for v in r) for r in batch]
            if not rows:
                continue
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=batch_size)
            total += len(rows)
            if total % 50_000 < batch_size:
                logger.info("Inserted %d rows so far", total)
        conn.commit()
    except Exception:
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
    """to_regclass existence + row count of the live target.

    Both identifiers must be double-quoted in the regclass argument: Postgres
    folds unquoted mixed-case to lowercase, so `public.Race` would resolve to
    `public.race` and always return NULL.
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


def swap_staging_into_target(
    conn,
    specs: Sequence[TableSyncSpec],
    *,
    lock_timeout: str = "10s",
    statement_timeout: str = "120s",
) -> None:
    """Atomic set-wise rename-swap: every spec's `_new` -> live, live -> `_old`,
    in ONE transaction.

    Live targets are locked upfront in spec order, a fixed order so concurrent
    lockers cannot deadlock. Each table then pre-drops any leftover `_old`
    (CASCADE, since old tables may hold FKs to each other), renames the live
    table and its indexes and constraints aside, and renames the staging table
    in with canonical names.

    The pre-drop matters because a leftover `_old` collides with every rename
    and wedges every subsequent run until dropped by hand.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
        cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")

        exists: dict[str, bool] = {}
        for spec in specs:
            cur.execute(
                "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename = %s",
                (spec.target_schema, spec.target_table),
            )
            exists[spec.target_table] = cur.fetchone() is not None
        for spec in specs:
            if exists[spec.target_table]:
                cur.execute(
                    f'LOCK TABLE "{spec.target_schema}"."{spec.target_table}" ' f"IN ACCESS EXCLUSIVE MODE"
                )

        for spec in specs:
            statements = [f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}" CASCADE']
            if exists[spec.target_table]:
                statements.append(
                    f'ALTER TABLE "{spec.target_schema}"."{spec.target_table}" '
                    f'RENAME TO "{spec.old_table}"'
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
                f'ALTER TABLE "{spec.staging_schema}"."{spec.new_table}" '
                f'SET SCHEMA "{spec.target_schema}"'
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

        conn.commit()
        logger.info(
            "Swap complete for %d tables: %s",
            len(specs),
            ", ".join(s.target_table for s in specs),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def drop_old_tables(conn, specs: Sequence[TableSyncSpec]) -> None:
    """DROP every `public.<table>_old` if it exists. Run after swap commits.

    CASCADE, because the old tables reference each other; dropping a parent
    cascades only to the FK constraints on other `_old` tables (live tables
    of the fresh vintage reference their own siblings, never `_old`).
    """
    cur = conn.cursor()
    try:
        for spec in specs:
            cur.execute(f'DROP TABLE IF EXISTS "{spec.target_schema}"."{spec.old_table}" CASCADE')
        conn.commit()
        logger.info("Dropped %d _old tables if they existed", len(specs))
    finally:
        cur.close()
