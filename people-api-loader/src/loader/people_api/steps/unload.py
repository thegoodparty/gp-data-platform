"""Step 1 — unload the people-api Voter mart from Databricks to S3 (DATA-1908).

Per state, issues `INSERT OVERWRITE DIRECTORY ... USING csv` against the SQL warehouse, writing
CSV files the `copy` step imports via `aws_s3.table_import_from_s3` (FORMAT csv). Columns are
ordered from `target_schema.sql` so file layout matches create-schema/copy by construction;
the declared Prisma-extra columns the mart lacks are emitted as NULL. Records an UnloadManifest.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.aws import s3
from loader.core.databricks import run_statement
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import (
    UnloadFile,
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema import unload_sql
from loader.people_api.schema.schema_spec import TABLE_SPECS
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.states import STATES
from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables

log = get_logger(__name__)

_TARGET_TABLE = "Voter"


def _list_state_files(cfg: LoaderConfig, prefix: str, state: str) -> list[UnloadFile]:
    """Enumerate written part files for a state; row_count is best-effort (see spec)."""
    state_prefix = f"{prefix}/state={state}/"
    files: list[UnloadFile] = []
    paginator = s3(cfg).get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=cfg.s3_bucket, Prefix=state_prefix):
        for obj in page.get("Contents", []):
            files.append(UnloadFile(state=state, s3_key=obj["Key"], size_bytes=obj["Size"], row_count=0))
    return files


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    state_filter: str | None = None,
    skip_submit: bool = False,
) -> UnloadManifest:
    bind(run_date=run_date, step="unload")
    existing = read_manifest(cfg, run_date, "unload", UnloadManifest)
    if existing and existing.status == "complete" and state_filter is None:
        log.info("unload.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "unload"))
        return existing

    mart_fqn = cfg.mart_fqns[_TARGET_TABLE]
    create_sql = extract_create_tables(load_target_schema(cfg, run_date))[_TARGET_TABLE]
    ddl_columns = extract_column_names(create_sql)
    extras = {name for name, _type, _nullable in TABLE_SPECS[_TARGET_TABLE].extra_columns}
    exprs = unload_sql.select_exprs(ddl_columns, extras)
    column_types_pg = unload_sql.column_types_from_ddl(create_sql)

    states = [state_filter] if state_filter else list(STATES)
    prefix = cfg.export_prefix(run_date)
    started = datetime.now(UTC)
    log.info("unload.start", mart=mart_fqn, states=len(states), skip_submit=skip_submit)

    for state in states:
        s3_dir = f"s3://{cfg.s3_bucket}/{prefix}/state={state}/"
        sql = unload_sql.unload_statement(mart_fqn=mart_fqn, select_exprs=exprs, state=state, s3_dir=s3_dir)
        if skip_submit:
            log.info("unload.sql_preview", state=state, sql=sql)
            continue
        run_statement(cfg, sql, warehouse_id=cfg.databricks_warehouse_id)
        log.info("unload.state_written", state=state)

    per_state_row_counts: dict[str, int] = {}
    files: list[UnloadFile] = []
    if not skip_submit:
        count_resp = run_statement(
            cfg, unload_sql.count_by_state_statement(mart_fqn), warehouse_id=cfg.databricks_warehouse_id
        )
        for row in count_resp.result.data_array or []:
            per_state_row_counts[row[0]] = int(row[1])
        for state in states:
            files.extend(_list_state_files(cfg, prefix, state))

    manifest = UnloadManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        databricks_table=mart_fqn,
        columns=ddl_columns,
        column_types_pg=column_types_pg,
        per_state_row_counts=per_state_row_counts,
        files=files,
    )
    uri = write_manifest(cfg, manifest)
    log.info("unload.complete", uri=uri, states=len(states), files=len(files))
    return manifest
