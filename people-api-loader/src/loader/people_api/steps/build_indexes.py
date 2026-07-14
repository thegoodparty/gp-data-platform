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
from functools import partial
from queue import Empty, Queue
from threading import Event, Lock, Thread

import psycopg

from loader.core.aws import rds, retry_after_settle, wait_instance_class_applied
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
from loader.people_api.schema.schema_spec import (
    TABLE_SPECS,
    indexes_for,
    is_partitioned,
    partition_column,
    primary_key_for,
)
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

# A worker that loses its connection mid-build reconnects through the shared tunnel and retries the
# (idempotent) item, rather than failing the whole step on the first transient drop. Bounds a brief
# blip; a sustained outage still exhausts these and falls through to Airflow's step-level retry.
_WORKER_MAX_RECONNECTS = 5
_RECONNECT_BACKOFF_SECONDS = (2, 5, 15, 30, 30)  # per reconnect attempt

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
    # CA/TX/FL/NY) spread wider and the absolute tail shrinks. (Sized for 192-vCPU
    # db.r8g.48xlarge; lower if LOADER_INDEX_INSTANCE_CLASS is overridden to a smaller class.)
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


def _create_index(conn: psycopg.Connection, idx: IndexDef, *, partition_key: str | None) -> None:
    """Build one PK-adjacent unique index at the parent level.

    `partition_key` is the LIST-partition column ("State") for a partitioned table, appended to the
    unique so uniqueness is enforceable on the partitioned relation; `None` for a flat table, whose
    unique is built on its real columns with nothing appended.
    """
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
        # dict.fromkeys dedupes in case a future dump's unique already includes the partition key.
        # NOTE (cutover): for a partitioned table, appending the partition key turns
        # Voter_LALVOTERID_key into UNIQUE("LALVOTERID", "State"). The dbt write models upsert with
        # ON CONFLICT ("LALVOTERID") against the single-column constraint, so at cutover
        # they must change to ON CONFLICT ("LALVOTERID", "State") to match this index:
        #   dbt/project/models/write/write__l2_databricks_to_gp_api.py:774
        #   dbt/project/models/write/write__people_api_db.py:399
        # Do NOT land those edits before the partitioned schema is the serving DB — the
        # current DB still has the single-column unique and the composite target would
        # break live upserts. See the PR body's cutover note.
        # SIBLING NOTE (cutover): DistrictVoter is the same DATA-1855 divergence. Its PK
        # (district_id, voter_id) becomes (district_id, voter_id, "state") on this partitioned
        # table (DistrictVoter's partition column is lowercase "state", not Voter's "State"), so
        # the dbt DistrictVoter write path's ON CONFLICT must move to the composite key
        # at cutover, not before — for the same reason the current DB carries the narrower key.
        # A flat table (partition_key is None) appends nothing and keeps its real columns.
        keyed = [*idx.columns, partition_key] if partition_key is not None else list(idx.columns)
        cols = ", ".join(f'"{c}"' for c in dict.fromkeys(keyed))
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


def _plain_parent_only_sql(idx: IndexDef, table: str) -> str:
    """`CREATE INDEX ... ON ONLY <parent>` — the empty parent index (instant, no partition data)."""
    sql = _rewrite_index_sql(idx.sql).rstrip().rstrip(";")
    return sql.replace(f'ON public."{table}"', f'ON ONLY public."{table}"', 1)


def _plain_child_sql(idx: IndexDef, table: str, state: str) -> tuple[str, str]:
    """(child_index_name, `CREATE INDEX ... ON <partition>`) preserving method/expr/opclass/WHERE."""
    child = _child_index_name(idx.name, state)
    sql = _rewrite_index_sql(idx.sql).rstrip().rstrip(";")
    sql = sql.replace(f'"{idx.name}"', f'"{child}"', 1)
    sql = sql.replace(f'ON public."{table}"', f'ON public."{table}_{state}"', 1)
    return child, sql


def _create_plain_parent_only(conn: psycopg.Connection, idx: IndexDef) -> None:
    with conn.cursor() as cur:
        cur.execute(_plain_parent_only_sql(idx, idx.table))  # ty: ignore[no-matching-overload]


def _create_plain_flat(conn: psycopg.Connection, idx: IndexDef) -> None:
    """Build a plain index DIRECTLY on a flat (non-partitioned) table.

    No `ON ONLY`/child/attach machinery: a flat table has no partitions, so the index is a single
    build on the table itself (re-issued verbatim, made idempotent by `_rewrite_index_sql`).
    """
    with conn.cursor() as cur:
        cur.execute(_rewrite_index_sql(idx.sql))  # ty: ignore[no-matching-overload]
        log.info("indexes.built_flat", table=idx.table, name=idx.name)


def _build_and_attach_child(conn: psycopg.Connection, item: tuple[IndexDef, str]) -> None:
    """Build one partition's child index and attach it to the parent partitioned index.

    Attach is idempotent: pg_inherits records the child under its parent index once attached, so a
    partial-rerun (child built + attached before a crash) skips the re-attach rather than erroring.
    """
    idx, state = item
    child, child_sql = _plain_child_sql(idx, idx.table, state)
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


def _partition_sizes(
    cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None
) -> dict[str, int]:
    """Return {state: on-disk bytes} for each public."<table>_<state>" leaf partition.

    One catalog query, used to schedule the largest partitions first. relkind='r' excludes the
    partitioned parent (relkind='p'), so only leaf partitions are returned. The LIKE is anchored on
    `<table>_`, so a query for "Voter" never matches "DistrictVoter_*" (which starts differently).
    """
    sql = (
        "SELECT c.relname, pg_relation_size(c.oid) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        rf"WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname LIKE '{table}\_%'"
    )
    sizes: dict[str, int] = {}
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(sql)  # ty: ignore[no-matching-overload]
        for relname, size in cur.fetchall():
            sizes[relname.removeprefix(f"{table}_")] = size
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


def _analyze(cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None = None) -> None:
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(f'ANALYZE public."{table}"')  # ty: ignore[no-matching-overload]
        log.info("indexes.analyzed", table=table)


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


def _ensure_instance_class(cfg: LoaderConfig, run_date: str) -> None:
    """Scale the writer up to the index-build instance class if it is not already there.

    provision/create_schema/copy run on the smaller `load_instance_class`; build_indexes is the
    only CPU-bound step and needs the large `index_instance_class`. Mirror resize's modify, but wait
    for the class change to actually APPLY (not just for a single availability check) before
    returning: Aurora keeps reporting 'available' for a few seconds after the modify before it flips
    to 'modifying' and reboots (see `wait_instance_class_applied` in `loader.core.aws` — observed
    2026-07-09: the scale-up reboot killed the PK-add connection, failing the step until Airflow
    retried). Also tolerates a still-in-progress modify from a partial re-run
    (InvalidDBInstanceStateFault -> settle via the instance waiter -> re-issue). Idempotent: when
    the box is already the index class this is a no-op describe, so per-date re-runs are safe.
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
    retry_after_settle(
        _modify,
        fault_code="InvalidDBInstanceStateFault",
        settle=lambda: waiter.wait(
            DBInstanceIdentifier=instance_id, WaiterConfig={"Delay": 30, "MaxAttempts": 40}
        ),
    )
    wait_instance_class_applied(rds_client, instance_id, target)
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

    def _build_in_parallel(fn: Callable[..., None], items: list) -> None:
        """Run `fn(conn, item)` over items with a fixed pool of PERSISTENT connections.

        Each worker opens ONE connection and reuses it for many items. All connections share the
        single bastion tunnel `fwd` (see `open_new_tunnel`), so the whole step makes exactly one
        SSH handshake regardless of `parallelism` — a fresh tunnel per connection floods the
        bastion's sshd MaxStartups (which failed the 20260708 runs). A transient connection drop
        (`psycopg.OperationalError`/`InterfaceError`) requeues the interrupted (idempotent) item and
        reconnects through the existing `fwd`, up to `_WORKER_MAX_RECONNECTS` with backoff. Any
        other error (real DDL/logic), or reconnects exhausted, stops the pool and re-raises after
        join.
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
                for reconnect in range(_WORKER_MAX_RECONNECTS + 1):
                    if stop.is_set():
                        return
                    try:
                        with connect_new(cfg, run_date, forward=fwd) as conn:
                            with conn.cursor() as cur:
                                _apply_session(cur)
                            while not stop.is_set():
                                try:
                                    item = work.get_nowait()
                                except Empty:
                                    return
                                try:
                                    fn(conn, item)
                                except (psycopg.OperationalError, psycopg.InterfaceError):
                                    # Connection lost mid-item: requeue it (idempotent) and reconnect.
                                    work.put(item)
                                    raise
                            return  # stop was set by another worker
                    except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                        if reconnect == _WORKER_MAX_RECONNECTS or stop.is_set():
                            raise
                        log.warning(
                            "indexes.worker_reconnect",
                            attempt=reconnect + 1,
                            error=str(e)[:160],
                        )
                        time.sleep(_RECONNECT_BACKOFF_SECONDS[reconnect])
            except BaseException as e:  # non-connection error, or reconnects exhausted: stop the pool
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

    # Manifest aggregates across every table (Voter first, TABLE_SPECS order).
    all_index_specs: list[IndexSpec] = []
    constraints_added: list[str] = []
    analyzed_tables: list[str] = []

    # One bastion tunnel for the WHOLE step (all tables); every connection below multiplexes through
    # it (see _build_in_parallel). `fwd` is None when no bastion is configured (direct connections).
    with open_new_tunnel(cfg, run_date) as fwd:
        for table in TABLE_SPECS:  # dict preserves insertion order (Voter first) -> stable
            partitioned = is_partitioned(table)
            pcol = partition_column(table)  # the LIST-partition column ("State") or None (flat)

            pk = primary_key_for(table)
            pks: list[PrimaryKey] = []
            if pk is not None:
                cols = list(pk.columns)
                if partitioned:
                    # Partition key must be part of every PK/unique on the partitioned table (a PG
                    # requirement). dict.fromkeys dedupes in case a dump's PK already includes it.
                    assert pcol is not None  # is_partitioned guarantees a partition column
                    cols = list(dict.fromkeys([*cols, pcol]))
                pks = [PrimaryKey(table=pk.table, constraint=pk.constraint, columns=cols)]

            idxs = indexes_for(table)
            unique_idxs = [i for i in idxs if i.unique]
            plain_idxs = [i for i in idxs if not i.unique]
            log.info(
                "indexes.parsed",
                table=table,
                primary_keys=len(pks),
                indexes=len(idxs),
                partitioned=partitioned,
            )

            # 1. Primary key(s) and the unique index(es): parent-level builds (few; fast/one-off).
            #    Flat tables pass partition_key=None (no State append); partitioned pass the column.
            _build_in_parallel(_add_primary_key, pks)
            _build_in_parallel(partial(_create_index, partition_key=pcol), unique_idxs)

            if partitioned:
                # 2. Plain indexes PER PARTITION (the bulk, and the whole bottleneck): first the empty
                #    parent indexes (ON ONLY, instant), then the child index on each partition as an
                #    independent (index, partition) unit across the pool, attached to its parent.
                #    Fine-grained scheduling keeps every builder on useful small work instead of a
                #    serial 51-partition walk per index.
                _build_in_parallel(_create_plain_parent_only, plain_idxs)
                # Schedule the biggest partitions first so each CREATE INDEX launches into an open
                # worker pool and grabs its full worker request (see _order_children_largest_first);
                # avoids the ~1-worker starvation of the CA/TX/FL giants when they launch mid-flood.
                sizes = _partition_sizes(cfg, run_date, table, forward=fwd)
                children = _order_children_largest_first([(i, s) for i in plain_idxs for s in STATES], sizes)
                _build_in_parallel(_build_and_attach_child, children)
            else:
                # 2. Flat table: plain indexes build directly on the table (no partitions to walk).
                _build_in_parallel(_create_plain_flat, plain_idxs)

            # 3. ANALYZE this table.
            _analyze(cfg, run_date, table, forward=fwd)

            analyzed_tables.append(table)
            constraints_added.extend(p.constraint for p in pks)
            all_index_specs.extend(
                IndexSpec(
                    table=i.table,
                    index_name=i.name,
                    # A partitioned table's unique index carries the partition key (as built above);
                    # flat tables and plain indexes keep their parsed columns verbatim.
                    columns=(
                        list(dict.fromkeys([*i.columns, pcol]))
                        if (i.unique and partitioned and pcol is not None)
                        else i.columns
                    ),
                    unique=i.unique,
                    where=i.where,
                )
                for i in idxs
            )

        # 4. l2Type coverage — Voter-only (queries the Voter table's columns vs prod org_districts);
        #    run once, not per table.
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
        indexes=all_index_specs,
        constraints_added=constraints_added,
        analyzed_tables=analyzed_tables,
        l2type_coverage_missing=missing,
    )
    uri = write_manifest(cfg, manifest)
    log.info("indexes.complete", uri=uri, indexes=len(all_index_specs), pks=len(constraints_added))
    return manifest
