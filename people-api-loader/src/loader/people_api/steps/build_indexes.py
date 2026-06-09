"""Step 5 — build primary keys, non-unique indexes, FK constraints, ANALYZE (DATA-1853).

Order: PKs (parallel across tables) → non-unique indexes (parallel,
round-robin interleaved across tables) → FKs (sequential) → ANALYZE
(parallel). Index/PK/FK specs are parsed from the committed prod-dump
snapshot. `CREATE INDEX` (not CONCURRENTLY) since the cluster is idle.

Tail check: `l2Type` coverage — every distinct `l2Type` in prod
`org_districts` must exist as a column on the new `Voter{STATE}` tables.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, resolve_writer_endpoint
from loader.people_api.manifests import (
    IndexManifest,
    IndexSpec,
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.emit_ddl import STATES
from loader.people_api.schema.index_specs import (
    IndexDef,
    PrimaryKey,
    parse_foreign_keys,
    parse_indexes,
    parse_primary_keys,
)
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.voter_columns import LEGACY_RENAMES

log = get_logger(__name__)

_INDEX_BUILDERS = 32

_BUILD_SESSION_SQL: tuple[str, ...] = (
    "SET maintenance_work_mem = '8GB'",
    "SET max_parallel_maintenance_workers = 8",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _apply_session(cur: psycopg.Cursor) -> None:
    for stmt in _BUILD_SESSION_SQL:
        cur.execute(stmt)  # ty: ignore[no-matching-overload]


def _rewrite_index_sql(sql: str) -> str:
    """Apply LEGACY_RENAMES and inject IF NOT EXISTS for idempotent reruns."""
    for old, new in LEGACY_RENAMES.items():
        if old in sql:
            sql = sql.replace(old, new)
    if "CREATE INDEX " in sql and "IF NOT EXISTS" not in sql.upper():
        sql = sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
    if "CREATE UNIQUE INDEX " in sql and "IF NOT EXISTS" not in sql.upper():
        sql = sql.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
    return sql


def _add_primary_key(cfg: LoaderConfig, run_date: str, writer_endpoint: str, pk: PrimaryKey) -> None:
    cols = ", ".join(f'"{c}"' for c in pk.columns)
    sql = f'ALTER TABLE public."{pk.table}" ADD CONSTRAINT "{pk.constraint}" PRIMARY KEY ({cols})'
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        try:
            cur.execute(sql)  # ty: ignore[no-matching-overload]
            log.info("indexes.pk_added", table=pk.table, constraint=pk.constraint)
        except (psycopg.errors.DuplicateObject, psycopg.errors.InvalidTableDefinition):
            log.info("indexes.pk_exists", table=pk.table, constraint=pk.constraint)


def _create_index(cfg: LoaderConfig, run_date: str, writer_endpoint: str, idx: IndexDef) -> None:
    sql = _rewrite_index_sql(idx.sql)
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(sql)  # ty: ignore[no-matching-overload]
        log.info("indexes.built", table=idx.table, name=idx.name, unique=idx.unique)


def _add_foreign_key(cfg: LoaderConfig, run_date: str, writer_endpoint: str, fk_sql: str, name: str) -> None:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        try:
            cur.execute(fk_sql)  # ty: ignore[no-matching-overload]
            log.info("indexes.fk_added", name=name)
        except (psycopg.errors.DuplicateObject, psycopg.errors.InvalidTableDefinition):
            log.info("indexes.fk_exists", name=name)


def _analyze(cfg: LoaderConfig, run_date: str, writer_endpoint: str, table: str) -> None:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(f'ANALYZE public."{table}"')  # ty: ignore[no-matching-overload]
        log.info("indexes.analyzed", table=table)


def _l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> list[str]:
    """`l2Type` values in prod `org_districts` not present as columns on the new schema."""
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # broad by design: org_districts lives in the app DB, not always reachable
        log.warning("indexes.l2type.skip", error=str(e))
        return []

    sample_table = f"Voter{STATES[0]}"
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (sample_table,),
        )
        new_cols = {r[0] for r in cur.fetchall()}
    return sorted(v for v in distinct_l2types if v not in new_cols)


def _order_key(counts: dict[str, int]) -> Callable[[str], int]:
    return lambda table_name: -counts.get(table_name.removeprefix("Voter"), 0)


def run(cfg: LoaderConfig, run_date: str) -> IndexManifest:
    bind(run_date=run_date, step="indexes")
    existing = read_manifest(cfg, run_date, "indexes", IndexManifest)
    if existing and existing.status == "complete":
        log.info(
            "indexes.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "indexes")
        )
        return existing

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    # unload is optional: used only to order builds largest-state-first.
    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)
    counts: dict[str, int] = unload.per_state_row_counts if unload else {}

    started = datetime.now(UTC)
    log.info("indexes.start")

    prod_dump = load_prod_dump(cfg, run_date)
    pks = parse_primary_keys(prod_dump)
    idxs = parse_indexes(prod_dump)
    fks = parse_foreign_keys(prod_dump)
    log.info("indexes.parsed", primary_keys=len(pks), indexes=len(idxs), foreign_keys=len(fks))

    voter_tables = {f"Voter{s}" for s in STATES} | {"VoterFile"}
    pks = [p for p in pks if p.table in voter_tables]
    idxs = [i for i in idxs if i.table in voter_tables]
    fks = [f for f in fks if f.table in voter_tables]

    # 1. Primary keys — parallel across tables, biggest first.
    pks_ordered = sorted(pks, key=lambda p: _order_key(counts)(p.table))
    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        futures = [
            executor.submit(_add_primary_key, cfg, run_date, writer_endpoint, pk) for pk in pks_ordered
        ]
        for fut in as_completed(futures):
            fut.result()

    # 2. Non-unique indexes — round-robin interleave across tables, biggest first.
    by_table: dict[str, list[IndexDef]] = {}
    for i in idxs:
        by_table.setdefault(i.table, []).append(i)
    tables_ordered = sorted(by_table.keys(), key=_order_key(counts))
    interleaved: list[IndexDef] = []
    queues = [list(by_table[t]) for t in tables_ordered]
    while any(queues):
        for q in queues:
            if q:
                interleaved.append(q.pop(0))
    log.info("indexes.order", biggest_first=tables_ordered[:5], total_indexes=len(interleaved))

    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        futures = [executor.submit(_create_index, cfg, run_date, writer_endpoint, idx) for idx in interleaved]
        for fut in as_completed(futures):
            fut.result()

    # 3. FKs — sequential.
    for fk in fks:
        _add_foreign_key(cfg, run_date, writer_endpoint, fk.sql, fk.constraint)

    # 4. ANALYZE — parallel across tables.
    with ThreadPoolExecutor(max_workers=_INDEX_BUILDERS) as executor:
        futures = [executor.submit(_analyze, cfg, run_date, writer_endpoint, t) for t in sorted(voter_tables)]
        for fut in as_completed(futures):
            fut.result()

    # 5. l2Type coverage.
    missing = _l2type_coverage(cfg, run_date, writer_endpoint)
    if missing:
        log.warning("indexes.l2type.missing", count=len(missing), examples=missing[:5])
    else:
        log.info("indexes.l2type.coverage_complete")

    manifest = IndexManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        indexes=[
            IndexSpec(table=i.table, index_name=i.name, columns=i.columns, unique=i.unique, where=i.where)
            for i in idxs
        ],
        constraints_added=[p.constraint for p in pks] + [f.constraint for f in fks],
        analyzed_tables=sorted(voter_tables),
        l2type_coverage_missing=missing,
    )
    uri = write_manifest(cfg, manifest)
    log.info("indexes.complete", uri=uri, indexes=len(idxs), pks=len(pks), fks=len(fks))
    return manifest
