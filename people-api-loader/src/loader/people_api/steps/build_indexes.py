"""Step 5 — build the PK + indexes on the unified Voter table, then ANALYZE (DATA-1853).

Reads the PK, the LALVOTERID unique, and the plain indexes from `schema_spec`
(the pg_catalog-sourced `_serving_seed`) and applies them to the LIST-partitioned
public."Voter". `CREATE INDEX` (not CONCURRENTLY) since the cluster is idle.

Plain (non-unique) indexes are built PER PARTITION, not on the parent. `CREATE INDEX ON <parent>`
recurses through every partition serially inside one statement, so N concurrent builders each walk
all ~51 partitions in sequence — coarse units, poor load-balancing (measured ~30h). Instead we
`CREATE INDEX ... ON ONLY <parent>` (empty/instant), then build one child index per partition as
independent `(index, partition)` units scheduled across the pool, then `ATTACH` each. That is the
scheduling the 52-separate-table POC used to hit ~12h on the same hardware; when all children of a
parent index are attached, the parent index flips valid automatically.

The PK and the single LALVOTERID unique index stay parent-level builds: the PK is fast, and the one
unique is a single build — not worth the partitioned-unique/PK-constraint machinery. No FKs exist.
"""

from __future__ import annotations

import hashlib
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
from loader.people_api.schema.states import STATES

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


def _child_index_name(index_name: str, state: str) -> str:
    """Deterministic, unique, <=63-char (PG identifier limit) name for a partition's child index.

    Prefer the readable `<index>_<state>`; if that exceeds 63 chars (a few very long district
    index names + a 2-char state would), fall back to a hashed name unique per (index, state).
    """
    name = f"{index_name}_{state}"
    if len(name) <= 63:
        return name
    return f"ix_{hashlib.md5(index_name.encode()).hexdigest()[:12]}_{state}"


def _plain_parent_only_sql(idx: IndexDef) -> str:
    """`CREATE INDEX ... ON ONLY <parent>` — the empty parent index (instant, no partition data)."""
    sql = _rewrite_index_sql(idx.sql).rstrip().rstrip(";")
    return sql.replace(f'ON public."{_TARGET_TABLE}"', f'ON ONLY public."{_TARGET_TABLE}"', 1)


def _plain_child_sql(idx: IndexDef, state: str) -> tuple[str, str]:
    """(child_index_name, `CREATE INDEX ... ON <partition>`) preserving method/expr/opclass/WHERE."""
    child = _child_index_name(idx.name, state)
    sql = _rewrite_index_sql(idx.sql).rstrip().rstrip(";")
    sql = sql.replace(f'"{idx.name}"', f'"{child}"', 1)
    sql = sql.replace(f'ON public."{_TARGET_TABLE}"', f'ON public."{_TARGET_TABLE}_{state}"', 1)
    return child, sql


def _create_plain_parent_only(cfg: LoaderConfig, run_date: str, idx: IndexDef) -> None:
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(_plain_parent_only_sql(idx))  # ty: ignore[no-matching-overload]


def _build_and_attach_child(cfg: LoaderConfig, run_date: str, item: tuple[IndexDef, str]) -> None:
    """Build one partition's child index and attach it to the parent partitioned index.

    Attach is idempotent: pg_inherits records the child under its parent index once attached, so a
    partial-rerun (child built + attached before a crash) skips the re-attach rather than erroring.
    """
    idx, state = item
    child, child_sql = _plain_child_sql(idx, state)
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        _apply_session(cur)
        cur.execute(child_sql)  # ty: ignore[no-matching-overload]
        cur.execute(
            "SELECT 1 FROM pg_inherits WHERE inhrelid = %s::regclass",
            (f'public."{child}"',),
        )
        if cur.fetchone() is None:
            cur.execute(  # ty: ignore[no-matching-overload]
                f'ALTER INDEX public."{idx.name}" ATTACH PARTITION public."{child}"'
            )
        log.info("indexes.child_built", name=idx.name, state=state, child=child)


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

    unique_idxs = [i for i in idxs if i.unique]
    plain_idxs = [i for i in idxs if not i.unique]

    # 1. Primary key(s) and the unique index: parent-level builds (few; fast/one-off).
    _build_in_parallel(_add_primary_key, pks)
    _build_in_parallel(_create_index, unique_idxs)

    # 2. Plain indexes PER PARTITION (the bulk, and the whole bottleneck): first the empty parent
    #    indexes (ON ONLY, instant), then the child index on each partition as an independent
    #    (index, partition) unit across the pool, attached to its parent. Fine-grained scheduling
    #    keeps every builder on useful small work instead of a serial 51-partition walk per index.
    _build_in_parallel(_create_plain_parent_only, plain_idxs)
    _build_in_parallel(_build_and_attach_child, [(i, s) for i in plain_idxs for s in STATES])

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
