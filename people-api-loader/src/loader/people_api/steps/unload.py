"""Step 1 — unload the people-api marts from Databricks to S3 (DATA-1908 / DATA-2100).

Iterates the target tables (Voter + District family). State-partitioned tables (Voter,
DistrictVoter) unload per state under `{prefix}/{table}/state={s}/`; flat tables (District,
DistrictStats) unload their whole mart once under `{prefix}/{table}/data/`. Each issues
`INSERT OVERWRITE DIRECTORY ... USING csv` against the SQL warehouse, writing CSV files the
`copy` step imports via `aws_s3.table_import_from_s3` (FORMAT csv). Columns are ordered from
`target_schema.sql` so file layout matches create-schema/copy by construction; the declared
Prisma-extra columns the mart lacks are emitted as NULL, and DistrictStats' `buckets` struct is
rewritten (two fields renamed to camelCase) + to_json'd. Records an UnloadManifest.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loader.core.aws import s3
from loader.core.databricks import run_statement
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import (
    UnloadFile,
    UnloadManifest,
    UnloadTable,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema import unload_sql
from loader.people_api.schema.schema_spec import TABLE_SPECS, is_partitioned
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.states import STATES
from loader.people_api.schema.table_ddl import (
    extract_column_names,
    extract_column_types,
    extract_create_tables,
)

log = get_logger(__name__)


def _table_prefix(prefix: str, table: str, state: str | None) -> str:
    """S3 subdir for a table's unit: partitioned -> <prefix>/<table>/state=<s>/, flat -> <prefix>/<table>/data/."""
    return f"{prefix}/{table}/state={state}/" if state is not None else f"{prefix}/{table}/data/"


def _list_state_files(
    s3_client: Any, bucket: str, unit_prefix: str, table: str, state: str
) -> list[UnloadFile]:
    """Enumerate the written data part files under a table's unit prefix; row_count is best-effort.

    `unit_prefix` is the full S3 prefix for one unit (`{prefix}/{table}/state={s}/` for a
    partitioned state, `{prefix}/{table}/data/` for a flat table). `table`/`state` are stamped on
    each recorded file (`state=""` for a flat table). Only Spark data files (basename `part-*`) are
    recorded — never the writer's marker/sidecar files (`_SUCCESS`, `_committed_*`, `_started_*`,
    `.crc`). copy feeds every listed file straight to aws_s3.table_import_from_s3, so a stray
    non-data file would corrupt the load; filtering positively here (not just on size) is the robust
    guard. The S3 client is built once by the caller and reused across units.
    """
    files: list[UnloadFile] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=unit_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].rsplit("/", 1)[-1].startswith("part-"):
                continue
            files.append(
                UnloadFile(table=table, state=state, s3_key=obj["Key"], size_bytes=obj["Size"], row_count=0)
            )
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

    # state is interpolated into the unload SQL and the S3 path; only accept a known state code.
    if state_filter is not None and state_filter not in STATES:
        raise ValueError(f"--state must be one of the {len(STATES)} known state codes; got {state_filter!r}")

    create_tables = extract_create_tables(load_target_schema(cfg, run_date))
    prefix = cfg.export_prefix(run_date)
    started = datetime.now(UTC)
    # A --state run touches only partitioned tables' matching state; flat tables are unloaded whole
    # on a full run only (their partial-run output is not persisted anyway — see guard below).
    states = [state_filter] if state_filter else list(STATES)
    s3_client = s3(cfg) if not skip_submit else None  # built once, reused across tables/units
    log.info("unload.start", tables=len(TABLE_SPECS), states=len(states), skip_submit=skip_submit)

    tables: list[UnloadTable] = []
    for table, spec in TABLE_SPECS.items():
        mart_fqn = cfg.mart_fqns[table]
        create_sql = create_tables[table]
        ddl_columns = extract_column_names(create_sql)
        extras = {name for name, _type, _nullable in spec.extra_columns}
        # The DistrictVoter mart's columns differ from its serving names (mart `state` -> serving
        # "State"); build the serving->mart renames (identity pairs dropped) so the SELECT projects
        # `state` AS `State`. Empty for the other tables (their marts already match serving).
        pg_to_mart = {pg: mart for mart, pg in spec.mart_column_map.items()}
        renames = {pg: mart for pg, mart in pg_to_mart.items() if pg != mart}
        # DistrictStats' buckets struct is rewritten to camelCase + to_json'd on the way out.
        transforms = {"buckets": unload_sql.BUCKETS_TO_JSON_EXPR} if table == "DistrictStats" else None
        exprs = unload_sql.select_exprs(ddl_columns, extras, transforms=transforms, renames=renames)
        column_types_pg = extract_column_types(create_sql)

        row_counts: dict[str, int] = {}
        files: list[UnloadFile] = []

        if is_partitioned(table):
            # The unload reads the MART, so filter/group by the mart's partition column name
            # (DistrictVoter: mart `state`; Voter: `State`), not the serving name.
            serving_partition_col = spec.partition_by
            assert serving_partition_col is not None  # is_partitioned guarantees it
            mart_partition_col = pg_to_mart.get(serving_partition_col, serving_partition_col)
            for state in states:
                s3_dir = f"s3://{cfg.s3_bucket}/{_table_prefix(prefix, table, state)}"
                sql = unload_sql.unload_statement(
                    mart_fqn=mart_fqn,
                    select_exprs=exprs,
                    state=state,
                    s3_dir=s3_dir,
                    partition_col=mart_partition_col,
                )
                if skip_submit:
                    log.info("unload.sql_preview", table=table, state=state, sql=sql)
                    continue
                run_statement(cfg, sql, warehouse_id=cfg.databricks_warehouse_id)
                log.info("unload.state_written", table=table, state=state)
            if not skip_submit:
                # The per-state count is a full GROUP BY scan and only feeds the persisted full-run
                # manifest (validate's baseline). A --state run doesn't persist, so skip the scan.
                if state_filter is None:
                    count_resp = run_statement(
                        cfg,
                        unload_sql.count_by_state_statement(mart_fqn, mart_partition_col),
                        warehouse_id=cfg.databricks_warehouse_id,
                    )
                    # The GROUP BY counts every distinct partition value, but only `state in STATES`
                    # is ever unloaded/loaded (see the per-state loop above). Keep the baseline in
                    # step with what's loadable: drop NULL and out-of-STATES groups (a NULL key would
                    # also break the str-keyed manifest; a stray code would fail copy's completeness
                    # gate). Log the drop so a dirty mart is visible rather than silently ignored.
                    dropped: dict[Any, int] = {}
                    for row in (getattr(count_resp, "result", None) and count_resp.result.data_array) or []:
                        if row[0] in STATES:
                            row_counts[row[0]] = int(row[1])
                        else:
                            dropped[row[0]] = int(row[1])
                    if dropped:
                        log.warning("unload.count_dropped_non_state_groups", table=table, groups=dropped)
                assert s3_client is not None
                for state in states:
                    files.extend(
                        _list_state_files(
                            s3_client, cfg.s3_bucket, _table_prefix(prefix, table, state), table, state
                        )
                    )
        else:
            # Flat table: unload the whole mart once, keyed under state="". Only on a full run — a
            # --state run leaves flat tables untouched (and its manifest is never persisted).
            if state_filter is None:
                s3_dir = f"s3://{cfg.s3_bucket}/{_table_prefix(prefix, table, None)}"
                sql = unload_sql.unload_statement(mart_fqn=mart_fqn, select_exprs=exprs, s3_dir=s3_dir)
                if skip_submit:
                    log.info("unload.sql_preview", table=table, state="", sql=sql)
                else:
                    run_statement(cfg, sql, warehouse_id=cfg.databricks_warehouse_id)
                    log.info("unload.table_written", table=table)
                    count_resp = run_statement(
                        cfg,
                        unload_sql.count_all_statement(mart_fqn),
                        warehouse_id=cfg.databricks_warehouse_id,
                    )
                    data = (getattr(count_resp, "result", None) and count_resp.result.data_array) or []
                    if data:
                        row_counts = {"": int(data[0][0])}
                    assert s3_client is not None
                    files.extend(
                        _list_state_files(
                            s3_client, cfg.s3_bucket, _table_prefix(prefix, table, None), table, ""
                        )
                    )

        tables.append(
            UnloadTable(
                table=table,
                databricks_table=mart_fqn,
                partition_by=spec.partition_by,
                columns=ddl_columns,
                column_types_pg=column_types_pg,
                row_counts=row_counts,
                files=files,
            )
        )

    manifest = UnloadManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        tables=tables,
    )
    # Do NOT write the canonical (run_date, step)-keyed manifest for a partial/no-op run:
    # a state-filtered run covers one state (and no flat tables), and a skip_submit (dry-run) submits
    # nothing and lists no files. Persisting status="complete" in either case would poison a later
    # full run's skip-guard into returning this partial/empty manifest. Mirrors copy_s3.
    total_files = sum(len(t.files) for t in tables)
    if state_filter is not None or skip_submit:
        log.info("unload.partial", state=state_filter, skip_submit=skip_submit, files=total_files)
        return manifest
    uri = write_manifest(cfg, manifest)
    log.info("unload.complete", uri=uri, tables=len(tables), files=total_files)
    return manifest
