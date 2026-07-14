"""Step 4 — parallel COPY S3 → Aurora across every unload table (DATA-1851/DATA-2100).

A ThreadPoolExecutor issues one `aws_s3.table_import_from_s3` per file, targeting
`public."<table>"` for whichever table the file belongs to; the `"State"` column (when
present) comes from the data. PG's COPY is single-threaded per statement, so file-level
parallelism is the lever.

Column contract: we pass an EXPLICIT column list (derived from the committed, generated
`target_schema.sql` (emit-ddl output) via `extract_column_names`) rather than an empty list. An empty list makes COPY
map file columns positionally against the table's physical DDL order, which is a
silent-corruption trap here — the Prisma-managed columns `created_at`, `id`, and
`updated_at` sit MID-table (not at the end) on Voter, so any unload whose layout differs
would write data into the wrong columns. The explicit list pins the contract: the
unload (DATA-1907/unload, DATA-2100) must emit exactly each table's columns, in this
order. `id` (PK, no default) and `updated_at` (no default) must be present;
`created_at` has a default but is included for a faithful full-table copy.

Idempotency is per (table, unit) — a "unit" is a state for a partitioned table (Voter,
DistrictVoter) or the single "" unit for a flat table (District, DistrictStats), mirroring
UnloadFile/UnloadTable's state="" convention. For a partitioned unit: count rows for the
state vs the unload baseline. Equal → skip. Zero → load. Partial → DELETE that state's
rows, then reload. For a flat unit (state==""), the same count/delete/reload sequence
runs over the WHOLE table (no `WHERE "State"`).
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
from loader.people_api.schema.schema_spec import is_partitioned, partition_column
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.table_ddl import (
    extract_column_names,
    extract_column_types,
    extract_create_tables,
)

log = get_logger(__name__)

_DEFAULT_PARALLELISM = 128

# Arbitrary namespace for the per-(table, unit) pg_advisory_lock so it can't collide with
# any other advisory lock taken on the cluster.
_COPY_LOCK_NAMESPACE = 0x564F  # "VO"

_SESSION_SQL: tuple[str, ...] = (
    "SET synchronous_commit = off",
    "SET maintenance_work_mem = '4GB'",
    "SET work_mem = '256MB'",
    "SET statement_timeout = 0",
    "SET idle_in_transaction_session_timeout = 0",
)

# PG `aws_s3.table_import_from_s3` base options — CSV (tab-delimited) to match the unload's Spark
# CSV writer: quoting/escaping (both '"') handle embedded tab/newline/quote in free-text fields;
# NULL '' pairs with the unload's nullValue=''. MUST stay in sync with unload_sql._CSV_OPTIONS;
# test_format_contract.py pins the pairing so a one-sided edit fails CI. The per-run FORCE_NULL
# clause (see `_import_options`) is appended to this base at runtime.
_IMPORT_OPTIONS = "(FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8')"

# Target types that can legitimately hold an empty string. For these, an empty CSV field stays as
# '' (the null-vs-empty-string distinction is deliberate — see unload_sql). Every OTHER type
# (INTEGER, BOOLEAN, DATE, TIMESTAMPTZ, DOUBLE PRECISION, UUID, ...) cannot parse '', so its
# quoted-empty "" must import as NULL via FORCE_NULL, or the COPY fails the cast.
_TEXT_TYPE_PREFIXES = ("TEXT", "VARCHAR", "CHAR", "CHARACTER", "CITEXT", "BPCHAR", "NAME")


def _force_null_columns(column_types: dict[str, str]) -> list[str]:
    """Columns (in DDL order) whose target type cannot hold an empty string.

    The unload writes a genuine empty-string mart value as a quoted-empty CSV field (Spark's default
    emptyValue is '""'), and PG's `NULL ''` reads quoted-empty as '' — fine for TEXT, but a fatal
    cast error for INTEGER/BOOLEAN/DATE/etc. FORCE_NULL on exactly these typed columns converts their
    quoted-empty back to NULL while leaving text columns' empty strings intact.
    """
    forced: list[str] = []
    for col, typ in column_types.items():
        base = typ.upper().split("(", 1)[0].strip()
        if not base.startswith(_TEXT_TYPE_PREFIXES):
            forced.append(col)
    return forced


def _import_options(force_null_columns: list[str]) -> str:
    """Base CSV options with a FORCE_NULL clause for the typed (non-text) columns appended inside
    the option parens. With no typed columns, returns the base options unchanged."""
    if not force_null_columns:
        return _IMPORT_OPTIONS
    cols = ", ".join(f'"{c}"' for c in force_null_columns)
    return f"{_IMPORT_OPTIONS[:-1]}, FORCE_NULL ({cols}))"


def _copy_one_file(
    cfg: LoaderConfig, run_date: str, s3_key: str, table: str, column_list: str, options: str
) -> None:
    """Import one S3 file into `public."<table>"` on its own backend.

    `column_list` is the explicit, DDL-ordered column list (see module docstring);
    it must name exactly the columns the unload file contains, in file order.
    `options` is the COPY options string (base CSV + per-run FORCE_NULL), built once in `run()`.
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
                "table": f'public."{table}"',
                "columns": column_list,
                "options": options,
                "bucket": cfg.s3_bucket,
                "key": s3_key,
                "region": cfg.aws_region,
            },
        )


def _acquire_unit_lock(cur: psycopg.Cursor, table: str, state: str) -> None:
    """Take the session advisory lock for (table, state) (released when the conn closes).

    hashtext is fed a table-qualified key so two tables loading the same state don't
    share a lock — e.g. Voter:TX and DistrictVoter:TX must be independent.

    ::int4 cast is required: psycopg3 binds the Python int as bigint, and PG has no
    pg_advisory_lock(bigint, int4) overload — only (bigint) or (int4, int4).
    """
    cur.execute("SELECT pg_advisory_lock(%s::int4, hashtext(%s))", (_COPY_LOCK_NAMESPACE, f"{table}:{state}"))


def _count_state_rows(conn: psycopg.Connection, table: str, state: str) -> int:
    """Row count for a unit: whole-table when `state == ""` (flat table), else state-filtered.

    `table` comes from the unload manifest's known table names (TABLE_SPECS), not user
    input, so the f-string is safe — same rationale as inspect_prod._inspect_table. A non-empty
    `state` implies a partitioned table, so `partition_column(table)` is its real LIST-partition
    column (Voter->"State", DistrictVoter->"state") — never a hardcoded "State".
    """
    with conn.cursor() as cur:
        if state:
            pcol = partition_column(table)
            assert pcol is not None  # a non-empty state unit only exists for a partitioned table
            sql = f'SELECT count(*) FROM public."{table}" WHERE "{pcol}" = %s'
            cur.execute(sql, (state,))  # ty: ignore[invalid-argument-type]
        else:
            cur.execute(f'SELECT count(*) FROM public."{table}"')  # ty: ignore[no-matching-overload]
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _delete_state(conn: psycopg.Connection, table: str, state: str) -> None:
    """Delete a unit's rows: whole-table when `state == ""` (flat table), else state-filtered."""
    with conn.cursor() as cur:
        if state:
            pcol = partition_column(table)
            assert pcol is not None  # a non-empty state unit only exists for a partitioned table
            sql = f'DELETE FROM public."{table}" WHERE "{pcol}" = %s'
            cur.execute(sql, (state,))  # ty: ignore[invalid-argument-type]
        else:
            cur.execute(f'DELETE FROM public."{table}"')  # ty: ignore[no-matching-overload]


def _load_unit(
    *,
    cfg: LoaderConfig,
    run_date: str,
    table: str,
    state: str,
    expected_rows: int,
    s3_keys: list[str],
    parallelism: int,
    column_list: str,
    options: str,
) -> CopyTableResult:
    """Load one (table, unit): `state` is a state code for a partitioned table, or "" for a flat
    table's single whole-table unit. Idempotent count/skip/delete/reload sequence either way."""
    bind(table=table, state=state)
    started = time.time()

    # Hold a session advisory lock on (table, state) for the whole count→delete→load
    # sequence. Without it, two concurrent invocations could both read count=0 and
    # both load — silently doubling the unit's rows, since the PK/unique that would
    # reject dupes is not built until build-indexes runs later. hashtext() is computed
    # server-side, so the key is stable across processes (Python's hash() is not).
    # The lock releases automatically when lock_conn closes.
    with connect_new(cfg, run_date) as lock_conn:
        with lock_conn.cursor() as cur:
            _acquire_unit_lock(cur, table, state)

        actual = _count_state_rows(lock_conn, table, state)
        if actual == expected_rows and expected_rows > 0:
            log.info("copy.skip", table=table, state=state, rows=actual)
            return CopyTableResult(
                table=table,
                state=state,
                expected_rows=expected_rows,
                actual_rows=actual,
                files_loaded=0,
                seconds_elapsed=0.0,
            )
        if actual > 0:
            # Partial load (an exact match already returned above) — reset and reload.
            log.info(
                "copy.partial_reload", table=table, state=state, existing_rows=actual, expected=expected_rows
            )
            _delete_state(lock_conn, table, state)

        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            futures = {
                executor.submit(_copy_one_file, cfg, run_date, key, table, column_list, options): key
                for key in s3_keys
            }
            errors: list[tuple[str, Exception]] = []
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    fut.result()
                    log.info("copy.file_done", table=table, state=state, key=key)
                except Exception as e:  # broad by design: aggregate worker failures, re-raise below
                    log.error("copy.file_failed", table=table, state=state, key=key, error=str(e))
                    errors.append((key, e))
            if errors:
                raise RuntimeError(f"{table}:{state}: {len(errors)} files failed — first: {errors[0][1]!r}")

        actual = _count_state_rows(lock_conn, table, state)
    elapsed = time.time() - started
    log.info(
        "copy.unit_done",
        table=table,
        state=state,
        rows=actual,
        files=len(s3_keys),
        seconds=round(elapsed, 1),
    )
    return CopyTableResult(
        table=table,
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

    tables_ddl = extract_create_tables(load_target_schema(cfg, run_date))

    started = datetime.now(UTC)
    log.info(
        "copy.start",
        state_filter=state_filter,
        parallelism=parallelism,
        tables=len(unload.tables),
    )

    results: list[CopyTableResult] = []
    covered: set[tuple[str, str]] = set()
    expected_units: set[tuple[str, str]] = set()

    for unload_table in unload.tables:
        table = unload_table.table
        # Explicit column list (DDL order) so COPY maps by a pinned contract, not raw
        # position — see module docstring. Quote every name uniformly; "id" == id in PG.
        if table not in tables_ddl:
            raise RuntimeError(f'target_schema.sql has no CREATE TABLE public."{table}"')
        columns = extract_column_names(tables_ddl[table])
        if not columns:
            raise RuntimeError(f'could not parse any columns from the "{table}" DDL')
        column_list = ", ".join(f'"{c}"' for c in columns)

        # Non-text columns cannot parse an empty string, so their quoted-empty "" (an empty-string
        # mart value written by Spark) must import as NULL — otherwise the CSV cast fails. FORCE_NULL
        # them; text columns keep their empty strings. See `_force_null_columns` / `_import_options`.
        force_null = _force_null_columns(extract_column_types(tables_ddl[table]))
        options = _import_options(force_null)

        files_by_state: dict[str, list[str]] = {}
        for f in unload_table.files:
            if f.size_bytes == 0:
                continue
            files_by_state.setdefault(f.state, []).append(f.s3_key)

        if is_partitioned(table):
            if state_filter:
                if state_filter not in files_by_state:
                    raise RuntimeError(
                        f"--state {state_filter!r} requested but the unload manifest has no loadable "
                        f'files for table "{table}" (all zero-size or absent).'
                    )
                units_to_load = [state_filter]
            else:
                units_to_load = sorted(files_by_state.keys())
        else:
            # Flat table: whole-table unit "", loaded only on a full run — a --state run
            # doesn't apply to a table with no State dimension (mirrors unload's behavior).
            units_to_load = [] if state_filter else list(files_by_state.keys())

        table_expected = {s for s, count in unload_table.row_counts.items() if count > 0}
        expected_units |= {(table, s) for s in table_expected}

        for state in sorted(units_to_load, key=lambda s: -unload_table.row_counts.get(s, 0)):
            result = _load_unit(
                cfg=cfg,
                run_date=run_date,
                table=table,
                state=state,
                expected_rows=unload_table.row_counts.get(state, 0),
                s3_keys=files_by_state.get(state, []),
                parallelism=parallelism,
                column_list=column_list,
                options=options,
            )
            results.append(result)
            covered.add((table, state))

    all_loaded = covered >= expected_units

    manifest = CopyManifest(
        run_date=run_date,
        status="complete" if all_loaded else "in_progress",
        started_at=started,
        finished_at=datetime.now(UTC) if all_loaded else None,
        results=results,
    )
    if all_loaded:
        # Persist ONLY when every table is loaded — manifest existence is the
        # orchestration "step complete" signal (people-api-loader CLAUDE.md). We
        # never write an in_progress manifest, so a poller can't advance on partial data.
        uri = write_manifest(cfg, manifest)
        log.info("copy.complete", uri=uri, covered=len(covered), expected=len(expected_units))
    elif state_filter is None:
        # A full run that didn't cover every expected unit is an anomaly (a unit with rows
        # but no loadable files). Surface it; write no manifest.
        missing = sorted(expected_units - covered)
        raise RuntimeError(f"copy incomplete: {len(missing)} unit(s) not loaded: {missing[:10]}")
    else:
        # Intentional single-state (--state) load: not every table/unit is done, so no
        # manifest is written; a later full run completes it.
        log.info("copy.state_loaded", state=state_filter, covered=sorted(covered))
    return manifest
