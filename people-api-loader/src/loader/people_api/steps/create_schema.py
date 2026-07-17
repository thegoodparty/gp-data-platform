"""Step 3 — create all four serving tables on the new cluster (ClickUp DATA-2100).

Applies `CREATE TABLE` DDL for Voter, DistrictVoter, District, and DistrictStats,
extracted from the committed, generated `target_schema.sql` (from `loader emit-ddl`;
tables only — indexes/PK are deferred to build-indexes), after installing the
aws_s3/aws_commons extensions. Voter and DistrictVoter are LIST-partitioned by
"State" (one child partition per USState); District and DistrictStats are plain
flat tables.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new
from loader.people_api.manifests import (
    SchemaManifest,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.schema_spec import TABLE_SPECS, is_partitioned, partition_column
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.states import STATES
from loader.people_api.schema.table_ddl import extract_create_tables

log = get_logger(__name__)


def build_partitioned_ddl(
    create_sql: str, table: str, partition_col: str, states: Sequence[str]
) -> tuple[str, list[str]]:
    """Turn a plain CREATE TABLE into a LIST-partitioned parent + per-state children.

    Returns (parent_ddl, [child_ddl, ...]). Both parent and children use
    IF NOT EXISTS so a retry after partial creation doesn't fail.
    """
    parent = create_sql.rstrip()
    if not parent.endswith(");"):
        raise RuntimeError("unexpected CREATE TABLE shape; cannot add PARTITION BY")
    parent = parent[:-1].rstrip() + f' PARTITION BY LIST ("{partition_col}");'
    parent = _flat_ddl(parent, table)  # reuse the CREATE TABLE -> CREATE TABLE IF NOT EXISTS rewrite
    children = [
        f'CREATE TABLE IF NOT EXISTS public."{table}_{s}" PARTITION OF public."{table}" '
        f"FOR VALUES IN ('{s}');"
        for s in states
    ]
    return parent, children


def _flat_ddl(create_sql: str, table: str) -> str:
    """Plain retry-safe CREATE for a non-partitioned table."""
    return create_sql.replace(
        f'CREATE TABLE public."{table}"', f'CREATE TABLE IF NOT EXISTS public."{table}"', 1
    )


def run(cfg: LoaderConfig, run_date: str) -> SchemaManifest:
    bind(run_date=run_date, step="schema")
    existing = read_manifest(cfg, run_date, "schema", SchemaManifest)
    if existing and existing.status == "complete":
        log.info(
            "schema.skip",
            reason="manifest already complete",
            uri=manifest_uri(cfg, run_date, "schema"),
        )
        return existing

    started = datetime.now(UTC)
    log.info("schema.start")

    schema_sql = load_target_schema(cfg, run_date)
    tables = extract_create_tables(schema_sql)

    stmts: list[str] = []
    tables_created: list[str] = []
    for pg_table in TABLE_SPECS:  # dict preserves insertion order (Voter first) -> stable
        if pg_table not in tables:
            raise RuntimeError(
                f'target_schema.sql has no CREATE TABLE public."{pg_table}" (found: {sorted(tables)})'
            )
        create_sql = tables[pg_table]
        if is_partitioned(pg_table):
            col = partition_column(pg_table)
            assert col is not None  # is_partitioned guarantees it
            parent, child_stmts = build_partitioned_ddl(create_sql, pg_table, col, STATES)
            stmts.append(parent)
            stmts.extend(child_stmts)
            tables_created.append(pg_table)
            tables_created.extend(f"{pg_table}_{s}" for s in STATES)
        else:
            stmts.append(_flat_ddl(create_sql, pg_table))
            tables_created.append(pg_table)

    full_ddl = "\n".join(stmts)
    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", full_ddl)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(full_ddl))

    with connect_new(cfg, run_date) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        for stmt in stmts:
            with conn.cursor() as cur:
                cur.execute(stmt)  # ty: ignore[no-matching-overload]
        log.info("schema.ddl_applied")

    manifest = SchemaManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        target_schema_s3_uri=ddl_uri,
        tables_created=tables_created,
        column_diff_from_prod={},
    )
    uri = write_manifest(cfg, manifest)
    log.info("schema.complete", uri=uri, tables=len(tables_created))
    return manifest
