"""Step 3 — create target tables on the new cluster (ClickUp DATA-1910).

Loads the committed prod schema snapshot + Databricks column list, runs
`emit_ddl` to produce the merged schema, connects to the new cluster as
master, installs `aws_s3`/`aws_commons`, applies the DDL, and (when an unload
manifest is present) verifies the `Voter{STATE}` column set.

No indexes/PKs/FKs here — `build-indexes` (step 5) adds those after COPY.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, resolve_writer_endpoint
from loader.people_api.manifests import (
    SchemaManifest,
    UnloadManifest,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.emit_ddl import STATES, emit_target_schema
from loader.people_api.schema.snapshot import load_databricks_columns, load_prod_dump

log = get_logger(__name__)


def _verify_column_set(conn: psycopg.Connection, table: str, expected: list[str]) -> list[str]:
    """Return the symmetric difference between actual and expected columns."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position",
            (table,),
        )
        actual = [r[0] for r in cur.fetchall()]
    return sorted(set(expected).symmetric_difference(set(actual)))


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
    # unload may not have run yet; if present we use it to verify columns.
    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)

    started = datetime.now(UTC)
    log.info("schema.start")

    prod_dump = load_prod_dump(cfg, run_date)
    databricks_cols = load_databricks_columns(cfg, run_date)

    ddl = emit_target_schema(prod_dump, databricks_cols)
    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", ddl)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(ddl))

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        with conn.cursor() as cur:
            # psycopg's parameterless execute overloads require LiteralString (a
            # type-checker concept, not a runtime type); emit_target_schema returns
            # plain str, so ty can't prove it. It is safe — the DDL is assembled
            # from controlled literals, not user input.
            cur.execute(ddl)  # ty:ignore[no-matching-overload]
        log.info("schema.ddl_applied")

        tables_created = [f"Voter{s}" for s in STATES] + ["VoterFile"]

        column_diff: dict[str, list[str]] = {}
        if unload is not None:
            for state in STATES:
                table = f"Voter{state}"
                diff = _verify_column_set(conn, table, unload.columns)
                if diff:
                    column_diff[table] = diff
            if column_diff:
                log.warning(
                    "schema.column_diff",
                    tables=list(column_diff.keys())[:5],
                    total=len(column_diff),
                )
            else:
                log.info("schema.columns_verified", tables=len(STATES))

    manifest = SchemaManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        target_schema_s3_uri=ddl_uri,
        tables_created=tables_created,
        column_diff_from_prod=column_diff,
    )
    uri = write_manifest(cfg, manifest)
    log.info("schema.complete", uri=uri, tables=len(tables_created))
    return manifest
