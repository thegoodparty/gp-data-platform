"""Step 3 — create the Voter table on the new cluster (ClickUp DATA-1910).

Applies the `CREATE TABLE public."Voter"` extracted from the committed prod
snapshot (tables only — indexes/PK are deferred to build-indexes), after
installing the aws_s3/aws_commons extensions.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, resolve_writer_endpoint
from loader.people_api.manifests import (
    SchemaManifest,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.table_ddl import extract_create_tables

log = get_logger(__name__)

_TARGET_TABLE = "Voter"


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

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("schema.start")

    dump = load_prod_dump(cfg, run_date)
    tables = extract_create_tables(dump)
    if _TARGET_TABLE not in tables:
        raise RuntimeError(f'snapshot has no CREATE TABLE public."{_TARGET_TABLE}" (found: {sorted(tables)})')
    create_sql = tables[_TARGET_TABLE]

    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", create_sql)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(create_sql))

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        with conn.cursor() as cur:
            # create_sql is plain str (assembled from the snapshot, not user input);
            # psycopg's parameterless execute overloads want LiteralString.
            cur.execute(create_sql)  # ty: ignore[no-matching-overload]
        log.info("schema.ddl_applied")

    manifest = SchemaManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        target_schema_s3_uri=ddl_uri,
        tables_created=[_TARGET_TABLE],
        column_diff_from_prod={},
    )
    uri = write_manifest(cfg, manifest)
    log.info("schema.complete", uri=uri, tables=1)
    return manifest
