"""Step 4 — parallel COPY S3 → Aurora into the unified Voter table (DATA-1851).

A ThreadPoolExecutor issues one `aws_s3.table_import_from_s3` per file, all
targeting `public."Voter"`; the `"State"` column comes from the data. PG's COPY
is single-threaded per statement, so file-level parallelism is the lever.

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
from loader.people_api.db import connect_new, resolve_writer_endpoint
from loader.people_api.manifests import (
    CopyManifest,
    CopyTableResult,
    UnloadManifest,
    manifest_uri,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

_DEFAULT_PARALLELISM = 128
_TARGET_TABLE = "Voter"

_SESSION_SQL: tuple[str, ...] = (
    "SET synchronous_commit = off",
    "SET maintenance_work_mem = '4GB'",
    "SET work_mem = '256MB'",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)


def _copy_one_file(cfg: LoaderConfig, run_date: str, writer_endpoint: str, s3_key: str) -> None:
    """Import one S3 file into public."Voter" on its own backend."""
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        for stmt in _SESSION_SQL:
            cur.execute(stmt)  # ty: ignore[no-matching-overload]
        cur.execute(
            """
            SELECT aws_s3.table_import_from_s3(
                %(table)s,
                '',
                %(options)s,
                aws_commons.create_s3_uri(%(bucket)s, %(key)s, %(region)s)
            )
            """,
            {
                "table": f'public."{_TARGET_TABLE}"',
                "options": "(FORMAT text, DELIMITER E'\\t', NULL '\\N', ENCODING 'UTF8')",
                "bucket": cfg.s3_bucket,
                "key": s3_key,
                "region": cfg.aws_region,
            },
        )


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
    writer_endpoint: str,
    state: str,
    expected_rows: int,
    s3_keys: list[str],
    parallelism: int,
) -> CopyTableResult:
    bind(state=state)
    started = time.time()

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        actual = _count_state_rows(conn, state)
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
            _delete_state(conn, state)

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = {
            executor.submit(_copy_one_file, cfg, run_date, writer_endpoint, key): key for key in s3_keys
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

    with connect_new(cfg, run_date, writer_endpoint) as conn:
        actual = _count_state_rows(conn, state)
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

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info(
        "copy.start", state_filter=state_filter, parallelism=parallelism, writer_endpoint=writer_endpoint
    )

    files_by_state: dict[str, list[str]] = {}
    for f in unload.files:
        if f.size_bytes == 0:
            continue
        files_by_state.setdefault(f.state, []).append(f.s3_key)

    states_to_load = [state_filter] if state_filter else sorted(files_by_state.keys())

    # No manifest carry-forward: resume is DB-driven — `_load_state` re-counts each
    # state and skips those already fully loaded. A partial manifest is never
    # persisted (below), so `existing` is only ever a complete manifest (returned
    # above) or absent.
    results: list[CopyTableResult] = [
        _load_state(
            cfg=cfg,
            run_date=run_date,
            writer_endpoint=writer_endpoint,
            state=state,
            expected_rows=unload.per_state_row_counts.get(state, 0),
            s3_keys=files_by_state.get(state, []),
            parallelism=parallelism,
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
