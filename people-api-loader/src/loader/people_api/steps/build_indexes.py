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
import time
from collections.abc import Callable
from datetime import UTC, datetime
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import TYPE_CHECKING

import psycopg
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from botocore.client import BaseClient

from loader.core.aws import rds
from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, open_new_tunnel
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

# Default number of concurrent builders. All builders share ONE bastion tunnel (see
# `open_new_tunnel` + `_build_in_parallel`), so concurrency is bounded by the index instance, not
# the bastion's sshd MaxStartups. build_indexes is cleanly CPU-bound (measured 2026-07-08:
# pg_stat_activity showed all active backends running, zero IPC/LWLock waits), and most partitions
# are small so each child build uses ~1 core — so throughput tracks the builder count up to the
# vCPU count. 128 targets the ~192-vCPU db.r8g.48xlarge index instance. maintenance_work_mem is kept
# modest (below) so builders * mem stays well under RAM; lower --parallelism for a smaller instance.
_DEFAULT_BUILDERS = 128
_TARGET_TABLE = "Voter"

_BUILD_SESSION_SQL: tuple[str, ...] = (
    # 3 GB * up to 128 builders = ~384 GB, safe alongside the buffer pool on the 1.5 TiB r8g.48xl.
    "SET maintenance_work_mem = '3GB'",
    # Aurora defaults max_parallel_workers to ~vCPU/2 (=96 on the 192-vCPU index box), which caps
    # the build at ~125 active backends and leaves ~67 cores idle (measured 2026-07-09). Raise the
    # per-session pool ceiling so the tail (~33 leaders + parallel workers) fills the box.
    # max_parallel_workers is a user-context GUC, so a per-session SET is honored; max_worker_processes
    # is 384 (ample), so no reboot-class parameter change is needed.
    "SET max_parallel_workers = 176",
    # Widen per-build parallelism so the long-pole giant partition builds (common columns on
    # CA/TX/FL/NY) spread wider and the absolute tail shrinks.
    "SET max_parallel_maintenance_workers = 16",
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


def _add_primary_key(conn: psycopg.Connection, pk: PrimaryKey) -> None:
    cols = ", ".join(f'"{c}"' for c in pk.columns)
    sql = f'ALTER TABLE public."{pk.table}" ADD CONSTRAINT "{pk.constraint}" PRIMARY KEY ({cols})'
    with conn.cursor() as cur:
        # Idempotency (the step must be re-runnable for a given date): a table can hold only one
        # PK, so re-adding when one already exists raises 42P16 "multiple primary keys not allowed"
        # — NOT 42710 DuplicateObject — even when the constraint name matches. Pre-check instead of
        # catching, so a genuine bad-DDL 42P16 (e.g. a missing column) still surfaces on the ADD.
        cur.execute(
            "SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'",
            (f'public."{pk.table}"',),
        )
        existing = cur.fetchone()
        if existing is not None:
            log.info("indexes.pk_exists", table=pk.table, constraint=existing[0])
            return
        cur.execute(sql)  # ty: ignore[no-matching-overload]
        log.info("indexes.pk_added", table=pk.table, constraint=pk.constraint)


def _create_index(conn: psycopg.Connection, idx: IndexDef) -> None:
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
    with conn.cursor() as cur:
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


def _create_plain_parent_only(conn: psycopg.Connection, idx: IndexDef) -> None:
    with conn.cursor() as cur:
        cur.execute(_plain_parent_only_sql(idx))  # ty: ignore[no-matching-overload]


def _build_and_attach_child(conn: psycopg.Connection, item: tuple[IndexDef, str]) -> None:
    """Build one partition's child index and attach it to the parent partitioned index.

    Attach is idempotent: pg_inherits records the child under its parent index once attached, so a
    partial-rerun (child built + attached before a crash) skips the re-attach rather than erroring.
    """
    idx, state = item
    child, child_sql = _plain_child_sql(idx, state)
    with conn.cursor() as cur:
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


def _partition_sizes(cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None) -> dict[str, int]:
    """Return {state: on-disk bytes} for each public."Voter_<state>" leaf partition.

    One catalog query, used to schedule the largest partitions first. relkind='r' excludes the
    partitioned parent (relkind='p'), so only leaf partitions are returned.
    """
    sql = (
        "SELECT c.relname, pg_relation_size(c.oid) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        r"WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE 'Voter\_%'"
    )
    sizes: dict[str, int] = {}
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(sql)
        for relname, size in cur.fetchall():
            sizes[relname.removeprefix("Voter_")] = size
    return sizes


def _order_children_largest_first(
    units: list[tuple[IndexDef, str]], partition_bytes: dict[str, int]
) -> list[tuple[IndexDef, str]]:
    """Order (index, state) build units by their state's partition size, largest first.

    Postgres grants parallel workers at statement start against the shared pool, so a giant that
    launches into a busy pool is starved to ~1 worker for hours. Submitting the biggest partitions
    first lets each grab its full worker request; near-empty partitions backfill. `sorted` is
    stable, so equal-size states keep input order. States with no known size sort last (0).
    """
    return sorted(units, key=lambda u: partition_bytes.get(u[1], 0), reverse=True)


def _analyze(cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None = None) -> None:
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute('ANALYZE public."Voter"')
        log.info("indexes.analyzed", table=_TARGET_TABLE)


def _l2type_coverage(
    cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None = None
) -> list[str] | None:
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

    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='Voter'"
        )
        new_cols = {r[0] for r in cur.fetchall()}
    return sorted(v for v in distinct_l2types if v not in new_cols)


# A class-change modify does not take effect atomically: Aurora keeps reporting the instance
# 'available' for a few seconds after modify_db_instance before it flips to 'modifying' and reboots.
# A single db_instance_available waiter therefore returns on the STALE 'available' — and the delayed
# reboot then drops whatever connection build_indexes has already opened (observed 2026-07-09: the
# scale-up reboot killed the PK-add connection, failing the step until Airflow retried). Poll until
# the class is actually applied AND the instance has settled instead.
_CLASS_APPLY_POLL_SECONDS = 30
_CLASS_APPLY_MAX_POLLS = 80  # ~40 min, generous for a class-change reboot


def _wait_class_applied(rds_client: BaseClient, instance_id: str, target: str) -> None:
    for _ in range(_CLASS_APPLY_MAX_POLLS):
        inst = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0]
        pending = inst.get("PendingModifiedValues") or {}
        if (
            inst["DBInstanceClass"] == target
            and inst["DBInstanceStatus"] == "available"
            and "DBInstanceClass" not in pending
        ):
            return
        time.sleep(_CLASS_APPLY_POLL_SECONDS)
    raise RuntimeError(f"instance {instance_id} did not reach class {target} after scale-up")


def _ensure_instance_class(cfg: LoaderConfig, run_date: str) -> None:
    """Scale the writer up to the index-build instance class if it is not already there.

    provision/create_schema/copy run on the smaller `load_instance_class`; build_indexes is the
    only CPU-bound step and needs the large `index_instance_class`. Mirror resize's modify, but wait
    for the class change to actually APPLY (not just for a single availability check) before
    returning: Aurora keeps reporting 'available' for a few seconds after the modify before it flips
    to 'modifying' and reboots (see `_wait_class_applied`). Also tolerates a still-in-progress modify
    from a partial re-run (InvalidDBInstanceStateFault -> settle via the instance waiter ->
    re-issue). Idempotent: when the box is already the index class this is a no-op describe, so
    per-date re-runs are safe.
    """
    instance_id = cfg.new_writer_instance_id(run_date)
    target = cfg.index_instance_class
    rds_client = rds(cfg)
    current = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0][
        "DBInstanceClass"
    ]
    if current == target:
        log.info("indexes.instance_class_ok", instance=instance_id, instance_class=target)
        return

    waiter = rds_client.get_waiter("db_instance_available")

    def _modify() -> None:
        rds_client.modify_db_instance(
            DBInstanceIdentifier=instance_id, DBInstanceClass=target, ApplyImmediately=True
        )

    log.info("indexes.scale_up", instance=instance_id, from_class=current, to_class=target)
    try:
        _modify()
    except ClientError as e:
        # Only tolerate a still-in-progress modify from a partial re-run; re-issue after it settles
        # so our class is actually applied. A genuine bad state re-raises on the retry.
        if e.response["Error"]["Code"] != "InvalidDBInstanceStateFault":
            raise
        log.warning("indexes.scale_up_retry_after_settle")
        waiter.wait(DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        _modify()
    _wait_class_applied(rds_client, instance_id, target)
    log.info("indexes.scale_up_applied", instance=instance_id, instance_class=target)


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

    # Scale the writer up to the index box before any build work; provision/copy ran on the
    # smaller load box. No-op on a re-run where the box is already scaled.
    _ensure_instance_class(cfg, run_date)

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
        """Run `fn(conn, item)` over items with a fixed pool of PERSISTENT connections, fail-fast.

        Each worker opens ONE connection and reuses it for many items. All connections share the
        single bastion tunnel `fwd` (see `open_new_tunnel`), so the whole step makes exactly one
        SSH handshake regardless of `parallelism` — a fresh tunnel per connection floods the
        bastion's sshd MaxStartups (which failed the 20260708 runs). On the first worker error the
        pool stops and re-raises it.
        """
        if not items:
            return
        work: Queue[object] = Queue()
        for it in items:
            work.put(it)
        errors: list[BaseException] = []
        errors_lock = Lock()
        stop = Event()

        def _worker() -> None:
            try:
                with connect_new(cfg, run_date, forward=fwd) as conn:
                    with conn.cursor() as cur:
                        _apply_session(cur)
                    while not stop.is_set():
                        try:
                            item = work.get_nowait()
                        except Empty:
                            return
                        fn(conn, item)
            except BaseException as e:  # record first error, stop the pool, re-raise after join
                with errors_lock:
                    errors.append(e)
                stop.set()

        threads = [Thread(target=_worker, name=f"idx-{i}") for i in range(min(parallelism, len(items)))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise errors[0]

    unique_idxs = [i for i in idxs if i.unique]
    plain_idxs = [i for i in idxs if not i.unique]

    # One bastion tunnel for the whole step; every connection below multiplexes through it (see
    # _build_in_parallel). `fwd` is None when no bastion is configured (direct connections).
    with open_new_tunnel(cfg, run_date) as fwd:
        # 1. Primary key(s) and the unique index: parent-level builds (few; fast/one-off).
        _build_in_parallel(_add_primary_key, pks)
        _build_in_parallel(_create_index, unique_idxs)

        # 2. Plain indexes PER PARTITION (the bulk, and the whole bottleneck): first the empty
        #    parent indexes (ON ONLY, instant), then the child index on each partition as an
        #    independent (index, partition) unit across the pool, attached to its parent. Fine-
        #    grained scheduling keeps every builder on useful small work instead of a serial
        #    51-partition walk per index.
        _build_in_parallel(_create_plain_parent_only, plain_idxs)
        # Schedule the biggest partitions first so each CREATE INDEX launches into an open worker
        # pool and grabs its full worker request (see _order_children_largest_first); avoids the
        # ~1-worker starvation of the CA/TX/FL giants when they launch mid-flood.
        sizes = _partition_sizes(cfg, run_date, forward=fwd)
        children = _order_children_largest_first([(i, s) for i in plain_idxs for s in STATES], sizes)
        _build_in_parallel(_build_and_attach_child, children)

        # 3. ANALYZE.
        _analyze(cfg, run_date, forward=fwd)

        # 4. l2Type coverage.
        missing = _l2type_coverage(cfg, run_date, forward=fwd)
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
