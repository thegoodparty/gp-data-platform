"""Shared build-and-swap lifecycle for syncing Databricks marts into the
election-api Postgres database.

Per table, a pipeline:

1. Drops and recreates `staging."<Target>_new"` with the same column shape as
   the live `public."<Target>"` table (no indexes — fast bulk-insert).
2. Streams the source mart from Databricks into the staging table.
3. Adds the PK, indexes, and FK constraints (canonical Prisma names suffixed
   with `_new`), all generated from the spec's declarative index/FK defs.
   Cross-table FKs reference the sibling STAGING table, never the live one,
   so the staging set is self-contained: it validates against its own
   vintage and never blocks or follows a live table.
4. Runs quality checks against the staging table (generic count /
   column-contract / id-overlap / NULL-probe gates plus per-table extras).

Then ONE transaction swaps the entire set: for each table, the live table is
renamed aside (`<Target>_old`) and the staging table renamed in, with every
index and constraint renamed to its canonical Prisma name. Constraints track
table identity through renames, so the staging-to-staging FKs arrive in
public pointing at their fresh siblings, and the old set's FKs leave with
the old tables — referential integrity holds within each vintage and no
child row is ever mutated. A crash anywhere rolls the whole set back; the
API only ever sees one complete vintage. Leftover `_old` tables from a run
that crashed before drop_old are pre-dropped inside the same transaction, so
a crashed run never wedges subsequent ones.

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

        if gate.min_id_overlap is not None and prior_count > 0:
            cur.execute(
                f'SELECT count(stg."{spec.pk_column}") '
                f'FROM "{spec.target_schema}"."{spec.target_table}" live '
                f'JOIN "{spec.staging_schema}"."{spec.new_table}" stg '
                f'ON live."{spec.pk_column}" = stg."{spec.pk_column}"'
            )
            overlap = cur.fetchone()[0]
            if overlap / prior_count < gate.min_id_overlap:
                raise ValueError(
                    f"{spec.target_table}: staged id overlap {overlap}/{prior_count} "
                    f"below floor {gate.min_id_overlap}; wholesale re-key "
                    f"suspected — refusing to swap"
                )

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
    """Drop and recreate `staging.<table>_new` matching `public.<table>` columns.

    CASCADE, because sibling staging tables may hold FKs to this one from a
    prior run. That drops the sibling's FK constraint (not the sibling); if
    the sibling's own rebuild does not restore it, the swap fails closed on
    the missing constraint rename rather than delivering a table without its
    FK.
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


def swap_staging_into_target(
    conn,
    specs: Sequence[TableSyncSpec],
    *,
    lock_timeout: str = "10s",
    statement_timeout: str = "120s",
) -> None:
    """Atomic set-wise rename-swap: every spec's `_new` -> live, live -> `_old`,
    in ONE transaction.

    All live targets are locked upfront in spec order (fixed order prevents
    deadlocks between concurrent lockers), then each table is processed in
    turn: pre-drop any leftover `_old` (CASCADE — leftover old tables may
    hold FKs to each other), rename the live table and its indexes and
    constraints to archive names, rename the staging table in with canonical
    names. Constraints track table identity through renames, so the staging
    set's internal FKs arrive pointing at the fresh siblings and the old
    set's FKs leave with it. No child row is ever mutated. A crash anywhere
    rolls the entire set back — the API only ever sees one complete vintage.

    A leftover `_old` would otherwise collide with every rename (table name
    and the `_old`-suffixed index/constraint names), failing each subsequent
    run until someone dropped it by hand — and five consecutive failed runs
    auto-pause the DAG. DDL is transactional, so a mid-swap failure rolls the
    pre-drop back with the renames.
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
