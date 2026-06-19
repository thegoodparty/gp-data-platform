"""Step 5 — build the PK + indexes on the unified Voter table, then ANALYZE (DATA-1853).

Reads the PK, the LALVOTERID unique, and the plain indexes from `schema_spec`
(the pg_catalog-sourced `_serving_seed`) and applies them to public."Voter" in
parallel (concurrent CREATE INDEX on one table run in separate sessions).
`CREATE INDEX` (not CONCURRENTLY) since the cluster is idle. No FKs exist in this schema.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod
from loader.people_api.manifests import (
    IndexManifest,
    IndexSpec,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.index_specs import IndexDef, PrimaryKey
from loader.people_api.schema.schema_spec import indexes_for, primary_key_for

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


def _add_primary_key(cfg: LoaderConfig, run_date: str, pk: PrimaryKey) -> None:
    cols = ", ".join(f'"{c}"' for c in pk.columns)
    sql = f'ALTER TABLE public."{pk.table}" ADD CONSTRAINT "{pk.constraint}" PRIMARY KEY ({cols})'
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        _apply_session(cur)
        try:
            cur.execute(sql)  # ty: ignore[no-matching-overload]
            log.info("indexes.pk_added", table=pk.table, constraint=pk.constraint)
        except psycopg.errors.DuplicateObject:
            # Only "constraint already exists" (42710) is idempotency. We deliberately do
            # NOT catch InvalidTableDefinition (42P16) — that means PG rejected the DDL
            # (e.g. a column doesn't exist), so it must propagate, not be recorded as added.
            log.info("indexes.pk_exists", table=pk.table, constraint=pk.constraint)


def _create_index(cfg: LoaderConfig, run_date: str, idx: IndexDef) -> None:
    if idx.unique:
        # We rebuild a unique index from its parsed columns (to append the partition
        # key), requoting each as an identifier. Empty columns would silently emit a
        # wrong UNIQUE("State") — fail loudly instead (guards an extraction regression).
        if not idx.columns:
            raise RuntimeError(
                f'unique index "{idx.name}" has no parsed columns; cannot rebuild it with the '
                "partition key without dropping its real columns"
            )
        # Rebuilding is only valid for plain columns: an expression index (e.g. lower("c"))
        # would become invalid DDL. The current seed has none; fail loudly if one appears.
        expr_cols = [c for c in idx.columns if "(" in c]
        if expr_cols:
            raise RuntimeError(
                f'cannot rebuild unique index "{idx.name}" from parsed columns: expression '
                f"column(s) {expr_cols} would produce invalid DDL when requoted with the "
                "partition key — this index needs manual handling"
            )
        # dict.fromkeys dedupes in case a future dump's unique already includes "State".
        # NOTE (cutover): appending "State" turns Voter_LALVOTERID_key into
        # UNIQUE("LALVOTERID", "State"). The dbt write models upsert with
        # ON CONFLICT ("LALVOTERID") against the single-column constraint, so at cutover
        # they must change to ON CONFLICT ("LALVOTERID", "State") to match this index:
        #   dbt/project/models/write/write__l2_databricks_to_gp_api.py:774
        #   dbt/project/models/write/write__people_api_db.py:399
        # Do NOT land those edits before the partitioned schema is the serving DB — the
        # current DB still has the single-column unique and the composite target would
        # break live upserts. See the PR body's cutover note.
        cols = ", ".join(f'"{c}"' for c in dict.fromkeys([*idx.columns, "State"]))
        # Preserve a partial-index predicate so we don't rebuild a broader unique than prod.
        where_clause = f" WHERE {idx.where}" if idx.where else ""
        sql = f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx.name}" ON public."{idx.table}" ({cols}){where_clause}'
    else:
        sql = _rewrite_index_sql(idx.sql)
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(sql)  # ty: ignore[no-matching-overload]
        log.info("indexes.built", table=idx.table, name=idx.name, unique=idx.unique)


def _analyze(cfg: LoaderConfig, run_date: str) -> None:
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        cur.execute('ANALYZE public."Voter"')
        log.info("indexes.analyzed", table=_TARGET_TABLE)


def _l2type_coverage(cfg: LoaderConfig, run_date: str) -> list[str] | None:
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

    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
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

    started = datetime.now(UTC)
    log.info("indexes.start")

    pk = primary_key_for(_TARGET_TABLE)
    pks = [pk] if pk is not None else []
    idxs = indexes_for(_TARGET_TABLE)
    log.info("indexes.parsed", primary_keys=len(pks), indexes=len(idxs))

    # Partition key "State" must be part of every PK/unique on the partitioned table.
    # dict.fromkeys dedupes in case a future dump's PK already includes "State".
    pks = [
        PrimaryKey(table=p.table, constraint=p.constraint, columns=list(dict.fromkeys([*p.columns, "State"])))
        for p in pks
    ]

    def _build_in_parallel(fn: Callable[..., None], items: list) -> None:
        """Run `fn(cfg, run_date, item)` across items, fail-fast."""
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = [executor.submit(fn, cfg, run_date, item) for item in items]
            for fut in as_completed(futures):
                fut.result()

    # 1. Primary key(s), then 2. indexes (unique + plain) — parallel on the single table.
    _build_in_parallel(_add_primary_key, pks)
    _build_in_parallel(_create_index, idxs)

    # 3. ANALYZE.
    _analyze(cfg, run_date)

    # 4. l2Type coverage.
    missing = _l2type_coverage(cfg, run_date)
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
                # Unique indexes get the partition key appended (deduped, as built above).
                columns=list(dict.fromkeys([*i.columns, "State"])) if i.unique else i.columns,
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
