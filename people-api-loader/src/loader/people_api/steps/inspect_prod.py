"""Step 0 — inspect the current Present cluster for the validate baseline (DATA-1907).

Captures, per prod table (Voter + the District family), total and per-state row counts
plus L2 snapshot freshness (max(updated_at) per state). Voter's per-state counts are the
"old" baseline the validate step diffs the freshly loaded cluster against.

The schema dump is NOT produced here — the committed schema/data/prod_dump.sql is the
schema source of truth. Per-state counts are captured only for tables that actually have
a "State" column (detected at runtime), so a District table without one yields a total
count rather than erroring.

The CLI writes the manifest to s3://.../voter_export_{date}/_manifest/inspect.json; the
Airflow xcom the validate step reads is the DAG's concern (the manifest is the contract).
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_prod
from loader.people_api.manifests import (
    InspectManifest,
    TableInspection,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

# Voter is required; the District family is best-effort (may be absent on a cluster).
_REQUIRED_TABLE = "Voter"
_OPTIONAL_TABLES: tuple[str, ...] = ("DistrictVoter", "District", "DistrictStats")


def _has_column(cur: psycopg.Cursor, table: str, column: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s AND column_name=%s",
        (table, column),
    )
    return cur.fetchone() is not None


def _scalar_int(cur: psycopg.Cursor) -> int:
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _inspect_table(cur: psycopg.Cursor, table: str) -> TableInspection:
    # Table name is from a fixed constant tuple (not user input); f-string is safe here.
    cur.execute(f'SELECT count(*) FROM public."{table}"')  # ty: ignore[no-matching-overload]
    total = _scalar_int(cur)

    per_state: dict[str, int] = {}
    snapshot: dict[str, str] = {}
    if _has_column(cur, table, "State"):
        count_sql = f'SELECT "State", count(*) FROM public."{table}" GROUP BY "State"'
        cur.execute(count_sql)  # ty: ignore[no-matching-overload]
        per_state = {r[0]: int(r[1]) for r in cur.fetchall() if r[0] is not None}
        if _has_column(cur, table, "updated_at"):
            snap_sql = f'SELECT "State", max(updated_at) FROM public."{table}" GROUP BY "State"'
            cur.execute(snap_sql)  # ty: ignore[no-matching-overload]
            snapshot = {
                r[0]: r[1].isoformat() for r in cur.fetchall() if r[0] is not None and r[1] is not None
            }
    return TableInspection(
        table=table,
        total_row_count=total,
        per_state_row_counts=per_state,
        per_state_snapshot_dates=snapshot,
    )


def run(cfg: LoaderConfig, run_date: str) -> InspectManifest:
    bind(run_date=run_date, step="inspect")
    existing = read_manifest(cfg, run_date, "inspect", InspectManifest)
    if existing and existing.status == "complete":
        log.info(
            "inspect.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "inspect")
        )
        return existing

    started = datetime.now(UTC)
    log.info("inspect.start", prod_cluster_id=cfg.prod_cluster_id)

    tables: list[TableInspection] = []
    with connect_prod(cfg) as conn, conn.cursor() as cur:
        # Voter is required — let a failure propagate so we never write a partial
        # "complete" manifest missing the baseline validate depends on.
        voter = _inspect_table(cur, _REQUIRED_TABLE)
        tables.append(voter)
        log.info(
            "inspect.table",
            table=_REQUIRED_TABLE,
            total=voter.total_row_count,
            states=len(voter.per_state_row_counts),
        )
        # The District family is best-effort: a table absent on this cluster is skipped,
        # not fatal. connect_prod is autocommit, so a failed query doesn't poison the rest.
        for table in _OPTIONAL_TABLES:
            try:
                ti = _inspect_table(cur, table)
                tables.append(ti)
                log.info("inspect.table", table=table, total=ti.total_row_count)
            except Exception as e:  # broad by design: optional tables may be absent on a cluster
                log.warning("inspect.table_skip", table=table, error=str(e))

    manifest = InspectManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        prod_cluster_id=cfg.prod_cluster_id,
        tables=tables,
    )
    uri = write_manifest(cfg, manifest)
    log.info("inspect.complete", uri=uri, tables=len(tables))
    return manifest
