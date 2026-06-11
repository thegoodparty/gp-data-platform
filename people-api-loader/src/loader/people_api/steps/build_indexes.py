"""Step 5 — build the PK + indexes on the unified Voter table, then ANALYZE (DATA-1853).

Parses the PK, the LALVOTERID unique, and the plain indexes from the committed
prod snapshot and applies them to public."Voter" in parallel (concurrent
CREATE INDEX on one table run in separate sessions). `CREATE INDEX` (not
CONCURRENTLY) since the cluster is idle. No FKs exist in this schema.
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
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.index_specs import (
    IndexDef,
    PrimaryKey,
    parse_indexes,
    parse_primary_keys,
)
from loader.people_api.schema.snapshot import load_prod_dump

log = get_logger(__name__)

# Default concurrent CREATE INDEX builds. Peak memory is roughly parallelism *
# maintenance_work_mem (8GB, set below), so 32 is about 256 GB -- sized for the
# default db.r7g.16xlarge load instance (512 GiB). Lower it via the CLI
# --parallelism flag for a smaller load instance to avoid OOM.
_DEFAULT_BUILDERS = 32
_TARGET_TABLE = "Voter"

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
    """Inject IF NOT EXISTS so reruns are idempotent."""
    if "IF NOT EXISTS" in sql.upper():
        return sql
    if "CREATE UNIQUE INDEX " in sql:
        return sql.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
    if "CREATE INDEX " in sql:
        return sql.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
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
    if idx.unique:
        # dict.fromkeys dedupes in case a future dump's unique already includes "State".
        cols = ", ".join(f'"{c}"' for c in dict.fromkeys([*idx.columns, "State"]))
        sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx.name}" ON public."{idx.table}" ({cols})'
    else:
        sql = _rewrite_index_sql(idx.sql)
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(sql)  # ty: ignore[no-matching-overload]
        log.info("indexes.built", table=idx.table, name=idx.name, unique=idx.unique)


def _analyze(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> None:
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute('ANALYZE public."Voter"')
        log.info("indexes.analyzed", table=_TARGET_TABLE)


def _l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> list[str] | None:
    """l2Type values in prod org_districts not present as columns on Voter.

    Returns the missing list, or `None` when the check was skipped because
    `org_districts` was unreachable — distinct from `[]` ("all covered") so the
    manifest doesn't read as clean coverage when the check never ran.
    """
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # broad by design: org_districts lives in the app DB, not always reachable
        log.warning("indexes.l2type.skip", error=str(e))
        return None

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='Voter'"
        )
        new_cols = {r[0] for r in cur.fetchall()}
    return sorted(v for v in distinct_l2types if v not in new_cols)


def run(cfg: LoaderConfig, run_date: str, *, parallelism: int = _DEFAULT_BUILDERS) -> IndexManifest:
    bind(run_date=run_date, step="indexes")
    existing = read_manifest(cfg, run_date, "indexes", IndexManifest)
    if existing and existing.status == "complete":
        log.info(
            "indexes.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "indexes")
        )
        return existing

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("indexes.start")

    dump = load_prod_dump(cfg, run_date)
    pks = [p for p in parse_primary_keys(dump) if p.table == _TARGET_TABLE]
    idxs = [i for i in parse_indexes(dump) if i.table == _TARGET_TABLE]
    log.info("indexes.parsed", primary_keys=len(pks), indexes=len(idxs))

    # Partition key "State" must be part of every PK/unique on the partitioned table.
    pks = [PrimaryKey(table=p.table, constraint=p.constraint, columns=[*p.columns, "State"]) for p in pks]

    def _build_in_parallel(fn: Callable[..., None], items: list) -> None:
        """Run `fn(cfg, run_date, writer_endpoint, item)` across items, fail-fast."""
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = [executor.submit(fn, cfg, run_date, writer_endpoint, item) for item in items]
            for fut in as_completed(futures):
                fut.result()

    # 1. Primary key(s), then 2. indexes (unique + plain) — parallel on the single table.
    _build_in_parallel(_add_primary_key, pks)
    _build_in_parallel(_create_index, idxs)

    # 3. ANALYZE.
    _analyze(cfg, run_date, writer_endpoint)

    # 4. l2Type coverage.
    missing = _l2type_coverage(cfg, run_date, writer_endpoint)
    if missing is None:
        log.warning("indexes.l2type.skipped", reason="org_districts unreachable")
    elif missing:
        log.warning("indexes.l2type.missing", count=len(missing), examples=missing[:5])
    else:
        log.info("indexes.l2type.coverage_complete")

    manifest = IndexManifest(
        run_date=run_date,
        status="complete",
        started_at=started,
        finished_at=datetime.now(UTC),
        indexes=[
            IndexSpec(
                table=i.table,
                index_name=i.name,
                # Unique indexes get the partition key appended (as built above).
                columns=[*i.columns, "State"] if i.unique else i.columns,
                unique=i.unique,
                where=i.where,
            )
            for i in idxs
        ],
        constraints_added=[p.constraint for p in pks],
        analyzed_tables=[_TARGET_TABLE],
        l2type_coverage_missing=missing,
    )
    uri = write_manifest(cfg, manifest)
    log.info("indexes.complete", uri=uri, indexes=len(idxs), pks=len(pks))
    return manifest
