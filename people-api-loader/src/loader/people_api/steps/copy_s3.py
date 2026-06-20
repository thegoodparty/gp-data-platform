"""Step 4 — parallel COPY S3 → Aurora into the unified Voter table (DATA-1851).

A ThreadPoolExecutor issues one `aws_s3.table_import_from_s3` per file, all
targeting `public."Voter"`; the `"State"` column comes from the data. PG's COPY
is single-threaded per statement, so file-level parallelism is the lever.

Column contract: we pass an EXPLICIT column list (derived from the committed, generated
`target_schema.sql` (emit-ddl output) via `extract_column_names`) rather than an empty list. An empty list makes COPY
map file columns positionally against the table's physical DDL order, which is a
silent-corruption trap here — the Prisma-managed columns `created_at`, `id`, and
`updated_at` sit MID-table (not at the end), so any unload whose layout differs
would write data into the wrong columns. The explicit list pins the contract: the
unload (DATA-1907/unload, still a stub) must emit exactly these columns, in this
order. `id` (PK, no default) and `updated_at` (no default) must be present;
`created_at` has a default but is included for a faithful full-table copy.

Idempotency is per-state on the `"State"` column: count rows for the state vs
the unload baseline. Equal → skip. Zero → load. Partial → DELETE that state's
rows, then reload.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import psycopg

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new
from loader.people_api.manifests import (
    CopyManifest,
    CopyTableResult,
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables

log = get_logger(__name__)

_DEFAULT_PARALLELISM = 128
_TARGET_TABLE = "Voter"

# Arbitrary namespace for the per-state pg_advisory_lock so it can't collide with
# any other advisory lock taken on the cluster.
_COPY_LOCK_NAMESPACE = 0x564F  # "VO"

_SESSION_SQL: tuple[str, ...] = (
    "SET synchronous_commit = off",
    "SET maintenance_work_mem = '4GB'",
    "SET work_mem = '256MB'",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _copy_one_file(cfg: LoaderConfig, run_date: str, s3_key: str, column_list: str) -> None:
    """Import one S3 file into public."Voter" on its own backend.

    `column_list` is the explicit, DDL-ordered column list (see module docstring);
    it must name exactly the columns the unload file contains, in file order.
    """
    with connect_new(cfg, run_date) as conn, conn.cursor() as cur:
        for stmt in _SESSION_SQL:
            cur.execute(stmt)  # ty: ignore[no-matching-overload]
        cur.execute(
            """
            SELECT aws_s3.table_import_from_s3(
                %(table)s,
                %(columns)s,
                %(options)s,
                aws_commons.create_s3_uri(%(bucket)s, %(key)s, %(region)s)
            )
            """,
            {
                "table": f'public."{_TARGET_TABLE}"',
                "columns": column_list,
                "options": "(FORMAT text, DELIMITER E'\\t', NULL '\\N', ENCODING 'UTF8')",
                "bucket": cfg.s3_bucket,
                "key": s3_key,
                "region": cfg.aws_region,
            },
        )


def _acquire_state_lock(cur: psycopg.Cursor, state: str) -> None:
    """Take the session advisory lock for `state` (released when the conn closes).

    ::int4 cast is required: psycopg3 binds the Python int as bigint, and PG has no
    pg_advisory_lock(bigint, int4) overload — only (bigint) or (int4, int4).
    """
    cur.execute("SELECT pg_advisory_lock(%s::int4, hashtext(%s))", (_COPY_LOCK_NAMESPACE, state))


def _count_state_rows(conn: psycopg.Connection, state: str) -> int:
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM public."Voter" WHERE "State" = %s', (state,))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _delete_state(conn: psycopg.Connection, state: str) -> None:
    with conn.cursor() as cur:
        cur.execute('DELETE FROM public."Voter" WHERE "State" = %s', (state,))


def _load_state(
    *,
    cfg: LoaderConfig,
    run_date: str,
    state: str,
    expected_rows: int,
    s3_keys: list[str],
    parallelism: int,
    column_list: str,
) -> CopyTableResult:
    bind(state=state)
    started = time.time()

    # Hold a session advisory lock on the state for the whole count→delete→load
    # sequence. Without it, two concurrent invocations could both read count=0 and
    # both load — silently doubling the state's rows, since the PK/unique that would
    # reject dupes is not built until build-indexes runs later. hashtext() is computed
    # server-side, so the key is stable across processes (Python's hash() is not).
    # The lock releases automatically when lock_conn closes.
    with connect_new(cfg, run_date) as lock_conn:
        with lock_conn.cursor() as cur:
            _acquire_state_lock(cur, state)

        actual = _count_state_rows(lock_conn, state)
        if actual == expected_rows and expected_rows > 0:
            log.info("copy.skip", state=state, rows=actual)
            return CopyTableResult(
                table=_TARGET_TABLE,
                state=state,
                expected_rows=expected_rows,
                actual_rows=actual,
                files_loaded=0,
                seconds_elapsed=0.0,
            )
        if actual > 0:
            # Partial load (an exact match already returned above) — reset and reload.
            log.info("copy.partial_reload", state=state, existing_rows=actual, expected=expected_rows)
            _delete_state(lock_conn, state)

        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = {
                executor.submit(_copy_one_file, cfg, run_date, key, column_list): key for key in s3_keys
            }
            errors: list[tuple[str, Exception]] = []
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    fut.result()
                    log.info("copy.file_done", state=state, key=key)
                except Exception as e:  # broad by design: aggregate worker failures, re-raise below
                    log.error("copy.file_failed", state=state, key=key, error=str(e))
                    errors.append((key, e))
            if errors:
                raise RuntimeError(f"{state}: {len(errors)} files failed — first: {errors[0][1]!r}")

        actual = _count_state_rows(lock_conn, state)
    elapsed = time.time() - started
    log.info("copy.state_done", state=state, rows=actual, files=len(s3_keys), seconds=round(elapsed, 1))
    return CopyTableResult(
        table=_TARGET_TABLE,
        state=state,
        expected_rows=expected_rows,
        actual_rows=actual,
        files_loaded=len(s3_keys),
        seconds_elapsed=elapsed,
    )


def run(
    cfg: LoaderConfig,
    run_date: str,
    *,
    state_filter: str | None = None,
    parallelism: int = _DEFAULT_PARALLELISM,
) -> CopyManifest:
    bind(run_date=run_date, step="copy")
    existing = read_manifest(cfg, run_date, "copy", CopyManifest)
    if existing and existing.status == "complete" and state_filter is None:
        log.info("copy.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "copy"))
        return existing

    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)
    if unload is None or unload.status != "complete":
        raise RuntimeError("Step 4 requires a completed unload manifest.")

    # Explicit column list (DDL order) so COPY maps by a pinned contract, not raw
    # position — see module docstring. Quote every name uniformly; "id" == id in PG.
    tables = extract_create_tables(load_target_schema(cfg, run_date))
    if _TARGET_TABLE not in tables:
        raise RuntimeError(f'target_schema.sql has no CREATE TABLE public."{_TARGET_TABLE}"')
    columns = extract_column_names(tables[_TARGET_TABLE])
    if not columns:
        raise RuntimeError(f'could not parse any columns from the "{_TARGET_TABLE}" DDL')
    column_list = ", ".join(f'"{c}"' for c in columns)

    started = datetime.now(UTC)
    log.info(
        "copy.start",
        state_filter=state_filter,
        parallelism=parallelism,
        columns=len(columns),
    )

    files_by_state: dict[str, list[str]] = {}
    for f in unload.files:
        if f.size_bytes == 0:
            continue
        files_by_state.setdefault(f.state, []).append(f.s3_key)

    states_to_load = [state_filter] if state_filter else sorted(files_by_state.keys())
    if state_filter and state_filter not in files_by_state:
        raise RuntimeError(
            f"--state {state_filter!r} requested but the unload manifest has no loadable "
            "files for it (all zero-size or absent)."
        )

    # No manifest carry-forward: resume is DB-driven — `_load_state` re-counts each
    # state and skips those already fully loaded. A partial manifest is never
    # persisted (below), so `existing` is only ever a complete manifest (returned
    # above) or absent.
    results: list[CopyTableResult] = [
        _load_state(
            cfg=cfg,
            run_date=run_date,
            state=state,
            expected_rows=unload.per_state_row_counts.get(state, 0),
            s3_keys=files_by_state.get(state, []),
            parallelism=parallelism,
            column_list=column_list,
        )
        for state in sorted(states_to_load, key=lambda s: -unload.per_state_row_counts.get(s, 0))
    ]

    covered = {r.state for r in results}
    expected_states = {s for s, count in unload.per_state_row_counts.items() if count > 0}
    all_loaded = covered >= expected_states

    manifest = CopyManifest(
        run_date=run_date,
        status="complete" if all_loaded else "in_progress",
        started_at=started,
        finished_at=datetime.now(UTC) if all_loaded else None,
        results=results,
    )
    if all_loaded:
        # Persist ONLY when the whole table is loaded — manifest existence is the
        # orchestration "step complete" signal (people-api-loader CLAUDE.md). We
        # never write an in_progress manifest, so a poller can't advance on partial data.
        uri = write_manifest(cfg, manifest)
        log.info("copy.complete", uri=uri, covered=len(covered), expected=len(expected_states))
    elif state_filter is None:
        # A full run that didn't cover every expected state is an anomaly (a state
        # with rows but no loadable files). Surface it; write no manifest.
        missing = sorted(expected_states - covered)
        raise RuntimeError(f"copy incomplete: {len(missing)} state(s) not loaded: {missing[:10]}")
    else:
        # Intentional single-state (--state) load: the whole table isn't done, so no
        # manifest is written; a later full run completes it.
        log.info("copy.state_loaded", state=state_filter, covered=sorted(covered))
    return manifest
