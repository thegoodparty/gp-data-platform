"""Step 3 — create the Voter table on the new cluster (ClickUp DATA-1910).

Applies a partitioned `CREATE TABLE public."Voter"` (LIST partitioned by
"State") extracted from the committed prod snapshot (tables only — indexes/PK
are deferred to build-indexes), after installing the aws_s3/aws_commons
extensions. One child partition per USState is created.
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
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.states import STATES
from loader.people_api.schema.table_ddl import extract_create_tables

log = get_logger(__name__)

_TARGET_TABLE = "Voter"


def build_partitioned_ddl(create_sql: str, table: str, states: Sequence[str]) -> tuple[str, list[str]]:
    """Turn a plain CREATE TABLE into a LIST-partitioned parent + per-state children.

    Returns (parent_ddl, [child_ddl, ...]). Both parent and children use
    IF NOT EXISTS so a retry after partial creation doesn't fail.
    """
    parent = create_sql.rstrip()
    if not parent.endswith(");"):
        raise RuntimeError("unexpected CREATE TABLE shape; cannot add PARTITION BY")
    parent = parent[:-1].rstrip() + ' PARTITION BY LIST ("State");'
    parent = parent.replace(
        f'CREATE TABLE public."{table}"', f'CREATE TABLE IF NOT EXISTS public."{table}"', 1
    )
    children = [
        f'CREATE TABLE IF NOT EXISTS public."{table}_{s}" PARTITION OF public."{table}" '
        f"FOR VALUES IN ('{s}');"
        for s in states
    ]
    return parent, children


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
    if _TARGET_TABLE not in tables:
        raise RuntimeError(
            f'target_schema.sql has no CREATE TABLE public."{_TARGET_TABLE}" (found: {sorted(tables)})'
        )
    create_sql = tables[_TARGET_TABLE]

    # Build the partitioned parent DDL and per-state child partitions.
    parent, child_stmts = build_partitioned_ddl(create_sql, _TARGET_TABLE, STATES)
    full_ddl = "\n".join([parent, *child_stmts])

    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", full_ddl)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(full_ddl))

    with connect_new(cfg, run_date) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        with conn.cursor() as cur:
            cur.execute(parent)  # ty: ignore[no-matching-overload]
        for child_stmt in child_stmts:
            with conn.cursor() as cur:
                cur.execute(child_stmt)  # ty: ignore[no-matching-overload]
        log.info("schema.ddl_applied")

    tables_created = [_TARGET_TABLE] + [f"Voter_{s}" for s in STATES]
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
