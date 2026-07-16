"""Step 7 — validate the new cluster before handoff (ClickUp DATA-1911, DATA-2100).

Runs, PER TABLE in the unload manifest (Voter, District, DistrictStats, DistrictVoter):
1. row_counts_match_databricks — the unload-integrity gate: per-state count (GROUP BY "State")
   for a partitioned table (Voter, DistrictVoter), or a single whole-table count(*) for a flat
   table (District, DistrictStats), within ±10% of the unload baseline (did COPY load everything
   that was unloaded).
2. prod_row_counts_within_tolerance — the same ±10% shape against the inspect-prod baseline
   (sanity: refresh magnitude vs the current Present cluster). Fails closed if inspect-prod hasn't
   run for this run_date, or if the REQUIRED Voter baseline is somehow missing. An optional table
   absent from the prod baseline (e.g. DistrictStats, "not yet a serving table" — see
   schema_spec.py; inspect_prod treats the whole District family as best-effort) is skipped
   instead — there's no magnitude baseline to sanity-check.
3. schema_diff_clean — new columns equal prod columns. Skipped (not failed) when prod has no
   such table at all (same DistrictStats case: nothing to diff against).
4. index_constraint_diff_clean — every prod index present on new (trivially passes when prod has
   no indexes for an absent table).

Plus two Voter-only checks (these are inherently Voter concerns, not per-table):
5. sample_queries_pass — voterFile.util.ts-shaped queries return without error.
6. l2Type_coverage — every distinct org_districts l2Type maps to a Voter column.

Writes validate.json + a Markdown companion. all_passed=False blocks handoff
(cli.py exits non-zero).
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, open_new_tunnel
from loader.people_api.manifests import (
    InspectManifest,
    UnloadManifest,
    UnloadTable,
    ValidateManifest,
    ValidationCheck,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.schema_spec import is_partitioned, partition_column
from loader.people_api.schema.unload_sql import BUCKETS_OUTPUT_KEYS

log = get_logger(__name__)

# Per-state row-count gate: within ±10% of a baseline (decided 2026-06-09).
_ROW_COUNT_TOLERANCE = 0.10


def _within_tolerance(actual: int, expected: int) -> bool:
    """`actual` within ±_ROW_COUNT_TOLERANCE of `expected` — the shared count-gate band."""
    return expected * (1 - _ROW_COUNT_TOLERANCE) <= actual <= expected * (1 + _ROW_COUNT_TOLERANCE)


def _new_counts_by_state(
    cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None = None
) -> dict[str, int]:
    """Per-state row counts for `table` on the new cluster (partitioned tables only).

    Groups by the table's real LIST-partition column (spec-driven; both Voter and DistrictVoter
    use "State"). Callers only invoke this for partitioned tables, so `partition_column` is never
    None.
    """
    pcol = partition_column(table)
    assert pcol is not None  # only called for partitioned tables (see _count_gate_check)
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(  # ty: ignore[no-matching-overload]
            f'SELECT "{pcol}", count(*) FROM public."{table}" GROUP BY "{pcol}"'
        )
        # Drop a NULL-State group (the partition key is NOT NULL, so this is defensive,
        # matching inspect_prod): a None key would break JSON-keyed manifest output.
        return {row[0]: int(row[1]) for row in cur.fetchall() if row[0] is not None}


def _new_total_count(
    cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None = None
) -> int:
    """Whole-table row count for `table` on the new cluster (flat tables only)."""
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM public."{table}"')  # ty: ignore[no-matching-overload]
        row = cur.fetchone()
        return int(row[0]) if row is not None else 0


def _compare_counts(
    name: str, actual_by_state: dict[str, int], expected: dict[str, int], *, flag_unexpected: bool = True
) -> ValidationCheck:
    """Build a ±tolerance per-state count check from already-fetched actuals.

    `flag_unexpected` treats a state present in the table but not the baseline as a
    mismatch. True for the unload gate (a state never unloaded shouldn't appear); False
    for the prod gate, where a state added by a geographic expansion is legitimately
    present in the new cluster but absent from the older prod snapshot.
    """
    mismatches: dict[str, dict[str, int]] = {}
    for state, expected_count in expected.items():
        actual = actual_by_state.get(state, 0)
        if not _within_tolerance(actual, expected_count):
            mismatches[state] = {"expected": expected_count, "actual": actual}
    if flag_unexpected:
        for state, actual_count in actual_by_state.items():
            if state not in expected:
                mismatches[state] = {"expected": 0, "actual": actual_count}
    return ValidationCheck(
        name=name,
        passed=not mismatches,
        details={
            "states": len(expected),
            "tolerance": _ROW_COUNT_TOLERANCE,
            "mismatch_count": len(mismatches),
            "mismatches": dict(list(mismatches.items())[:10]),
        },
    )


def _compare_total(name: str, actual: int, expected: int) -> ValidationCheck:
    """Build a ±tolerance whole-table count check (flat tables: District, DistrictStats)."""
    return ValidationCheck(
        name=name,
        passed=_within_tolerance(actual, expected),
        details={"expected": expected, "actual": actual, "tolerance": _ROW_COUNT_TOLERANCE},
    )


def _count_gate_check(
    cfg: LoaderConfig, run_date: str, unload_table: UnloadTable, *, forward: tuple[str, int] | None = None
) -> tuple[ValidationCheck, dict[str, int] | int]:
    """One table's unload-integrity gate: per-state ±10% (partitioned) or whole-table ±10% (flat).

    Returns `(check, actual)` — `actual` is reused by `_check_prod_row_counts` so the new
    cluster's counts for this table are queried once, not twice.
    """
    name = f"row_counts_match_databricks:{unload_table.table}"
    if is_partitioned(unload_table.table):
        actual = _new_counts_by_state(cfg, run_date, unload_table.table, forward=forward)
        return _compare_counts(name, actual, unload_table.row_counts), actual
    actual_total = _new_total_count(cfg, run_date, unload_table.table, forward=forward)
    return _compare_total(name, actual_total, unload_table.row_counts.get("", 0)), actual_total


def _check_prod_row_counts(
    cfg: LoaderConfig, run_date: str, new_counts: dict[str, dict[str, int] | int]
) -> list[ValidationCheck]:
    """New-vs-prod ±10% gate per table, using inspect-prod's per-table baseline.

    Distinct from the unload gate (load integrity): this checks the refresh is roughly the same
    magnitude as the current Present cluster. Fails closed (like the other prod-dependent checks)
    when the inspect baseline is missing entirely, or when the REQUIRED Voter baseline is absent
    from a `complete` inspect manifest (shouldn't happen — inspect_prod guarantees it — but fail
    closed rather than silently pass). An OPTIONAL table (the District family) absent from the
    baseline is skipped: inspect_prod itself treats these as best-effort (e.g. DistrictStats is
    "not yet a serving table" on any current prod cluster), so there is no magnitude baseline to
    compare against — skipping avoids a permanently-failing gate for a legitimately new table.
    """
    inspect = read_manifest(cfg, run_date, "inspect", InspectManifest)
    if inspect is None or inspect.status != "complete":
        log.error("validate.prod_baseline_missing", reason="no completed inspect manifest")
        return [
            ValidationCheck(
                name=f"prod_row_counts_within_tolerance:{table}",
                passed=False,
                details={"error": "no completed inspect manifest for this run_date"},
            )
            for table in new_counts
        ]

    checks: list[ValidationCheck] = []
    for table, actual in new_counts.items():
        name = f"prod_row_counts_within_tolerance:{table}"
        insp = next((t for t in inspect.tables if t.table == table), None)
        if insp is None:
            if table == "Voter":
                log.error("validate.prod_baseline_missing", reason="no Voter baseline in inspect manifest")
                checks.append(
                    ValidationCheck(
                        name=name, passed=False, details={"error": "inspect manifest has no Voter baseline"}
                    )
                )
            else:
                log.warning("validate.prod_baseline_table_absent", table=table)
                checks.append(
                    ValidationCheck(
                        name=name, passed=True, details={"skipped": "table absent from inspect baseline"}
                    )
                )
            continue
        # `isinstance` (not `is_partitioned`) narrows `actual`'s union type for ty: `_count_gate_check`
        # guarantees a dict iff the table is partitioned, so this is equivalent by construction.
        if isinstance(actual, dict):
            if not insp.per_state_row_counts:
                if table == "Voter":
                    # Voter is REQUIRED: an empty per-state baseline is a fault -> fail closed.
                    log.error("validate.prod_baseline_missing", reason="no Voter per-state baseline")
                    checks.append(
                        ValidationCheck(
                            name=name,
                            passed=False,
                            details={"error": "inspect manifest has no Voter per-state baseline"},
                        )
                    )
                else:
                    # A non-Voter partitioned serving table (e.g. DistrictVoter) new to the loader:
                    # a current prod cluster may carry it with no usable per-state baseline. There's
                    # no magnitude baseline to assume, so SKIP rather than fail closed (mirrors the
                    # optional-table-absent skip above) — failing would wedge the gate for a legit
                    # new table. The unload gate already covers this table's load integrity.
                    log.warning("validate.prod_baseline_empty", table=table)
                    checks.append(
                        ValidationCheck(
                            name=name,
                            passed=True,
                            details={"skipped": f"{table} present but has no per-state prod baseline"},
                        )
                    )
                continue
            # Don't flag states new to the cluster but absent from the prod snapshot — that's a
            # legitimate geographic expansion, not drift (the unload gate already covers integrity).
            checks.append(_compare_counts(name, actual, insp.per_state_row_counts, flag_unexpected=False))
        elif insp.total_row_count == 0:
            # Flat serving table present but empty (0 rows) — no magnitude baseline to assume. SKIP
            # rather than compare a faithful load against 0 (which _within_tolerance(N>0, 0) would
            # fail). Flat tables are always optional (District/DistrictStats; Voter/DistrictVoter are
            # partitioned), so this mirrors the partitioned empty-baseline skip above.
            log.warning("validate.prod_baseline_empty", table=table)
            checks.append(
                ValidationCheck(
                    name=name,
                    passed=True,
                    details={"skipped": f"{table} present but has an empty (0-row) prod baseline"},
                )
            )
        else:
            checks.append(_compare_total(name, actual, insp.total_row_count))
    return checks


def _columns(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (table,),
        )
        return {r[0] for r in cur.fetchall()}


def _check_schema_diff(
    cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    name = f"schema_diff_clean:{table}"
    try:
        with connect_prod(cfg) as prod_conn:
            prod_cols = _columns(prod_conn, table)
    except Exception as e:  # broad by design: prod may be unreachable; record as a failed check
        log.error("validate.prod_unreachable", check=name, error=str(e))
        return ValidationCheck(name=name, passed=False, details={"error_reading_prod": str(e)})
    if not prod_cols:
        # No columns on prod: this table doesn't exist there yet (e.g. DistrictStats, which
        # schema_spec.py documents as "not yet a serving table" — inspect_prod treats the whole
        # District family as best-effort/optional for the same reason). Nothing to diff against;
        # skip rather than report every column on the new cluster as "extra" and fail forever.
        log.warning("validate.schema_diff.skip", table=table, reason="table absent from prod")
        return ValidationCheck(name=name, passed=True, details={"skipped": "table absent from prod"})
    with connect_new(cfg, run_date, forward=forward) as conn:
        new_cols = _columns(conn, table)
    missing_from_new = prod_cols - new_cols
    extra_in_new = new_cols - prod_cols
    # The loader partitions some tables by "State", so the new cluster has that partition column.
    # If prod's copy of a partitioned table doesn't have it yet, that lone extra is the INTENDED
    # partitioning divergence (the fresh table matches the newer Prisma; current serving is behind —
    # the same divergence tracked for the DATA-1855 cutover), not schema drift. Don't fail on it;
    # any OTHER column difference still fails.
    pcol = partition_column(table) if is_partitioned(table) else None
    allowed_extra = {pcol} if pcol and pcol not in prod_cols else set()
    unexpected_extra = extra_in_new - allowed_extra
    return ValidationCheck(
        name=name,
        passed=not missing_from_new and not unexpected_extra,
        details={
            "prod_cols": len(prod_cols),
            "new_cols": len(new_cols),
            "missing_from_new": sorted(missing_from_new)[:20],
            "extra_in_new": sorted(extra_in_new)[:20],
            "allowed_partition_extra": sorted(allowed_extra),
        },
    )


def _check_indexes(
    cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    name = f"index_constraint_diff_clean:{table}"
    query = "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename=%s ORDER BY indexname"
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute(query, (table,))
            prod_idx = {r[0] for r in cur.fetchall()}
    except Exception as e:  # broad by design: prod may be unreachable; record as a failed check
        log.error("validate.prod_unreachable", check=name, error=str(e))
        return ValidationCheck(name=name, passed=False, details={"error_reading_prod": str(e)})
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(query, (table,))
        new_idx = {r[0] for r in cur.fetchall()}
    missing = sorted(prod_idx - new_idx)
    return ValidationCheck(
        name=name,
        passed=not missing,
        details={"prod_count": len(prod_idx), "new_count": len(new_idx), "missing_from_new": missing[:20]},
    )


_SAMPLE_QUERIES: tuple[tuple[str, str], ...] = (
    ("party_filter", 'SELECT count(*) FROM public."Voter" WHERE "Parties_Description" = \'Democratic\''),
    # Use the materialized integer column, not "Age"::integer — a non-numeric
    # "Age" text value would error the whole query and fail the check on good data.
    ("age_filter", 'SELECT count(*) FROM public."Voter" WHERE "Age_Int" BETWEEN 18 AND 35'),
    ("state_filter", 'SELECT count(*) FROM public."Voter" WHERE "State" = \'TX\''),
    ("lalvoterid_lookup", 'SELECT count(*) FROM public."Voter" WHERE "LALVOTERID" IS NOT NULL'),
)


def _check_sample_queries(
    cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    results: dict[str, str] = {}
    failures: dict[str, str] = {}
    with connect_new(cfg, run_date, forward=forward) as conn:
        for label, sql in _SAMPLE_QUERIES:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)  # ty: ignore[no-matching-overload]
                    row = cur.fetchone()
                    results[label] = "ok" if row is not None else "empty"
            except Exception as e:  # broad by design: query failures are captured as check results
                failures[label] = str(e)
    return ValidationCheck(
        name="sample_queries_pass", passed=not failures, details={"pass": results, "fail": failures}
    )


# Selective point-lookups on Voter's PK / unique. Equality on a unique/PK column is estimated at
# ~1 row regardless of whether the value exists, so the planner uses the index — deterministic, not
# selectivity-flaky (unlike the count(*) sample queries, which the planner legitimately seq-scans
# for low-selectivity predicates). With no partition-key filter the plan is an Append over all
# partitions, so we assert an index scan is PRESENT (the index is analyzed + usable) rather than
# "no Seq Scan anywhere": the planner may legitimately seq-scan a tiny leaf partition that's cheaper
# to scan than to index, and that lone node must not fail the gate. A real regression (no ANALYZE /
# unusable index) yields NO index-scan node at all.
_INDEX_USAGE_QUERIES: tuple[tuple[str, str], ...] = (
    ("id_lookup", 'SELECT 1 FROM public."Voter" WHERE "id" = \'00000000-0000-0000-0000-000000000000\'::uuid'),
    ("lalvoterid_lookup", 'SELECT 1 FROM public."Voter" WHERE "LALVOTERID" = \'__validate_probe__\''),
)
_INDEX_SCAN_NODES = ("Index Scan", "Index Only Scan", "Bitmap Index Scan")


def _check_index_usage(
    cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    """EXPLAIN selective Voter point-lookups and confirm the planner serves them via an index.

    Beyond 'the index exists' (index_constraint_diff) and 'the index is valid' (indexes_valid), this
    is the performance gate: it proves the fresh cluster's indexes are actually PLANNED — i.e.
    ANALYZE ran and the planner has usable stats. A regression (missing ANALYZE, unusable index)
    shows up as a Seq Scan. EXPLAIN only (no execution), so it's cheap. New cluster only.
    """
    results: dict[str, str] = {}
    failures: dict[str, str] = {}
    with connect_new(cfg, run_date, forward=forward) as conn:
        for label, sql in _INDEX_USAGE_QUERIES:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"EXPLAIN {sql}")  # ty: ignore[no-matching-overload]
                    plan = "\n".join(r[0] for r in cur.fetchall())
            except Exception as e:  # broad by design: capture the failure as a check result
                failures[label] = str(e)
                continue
            uses_index = any(node in plan for node in _INDEX_SCAN_NODES)
            if uses_index:
                results[label] = "index"
            else:
                failures[label] = (plan.splitlines() or ["(no plan)"])[0]
    return ValidationCheck(
        name="index_usage", passed=not failures, details={"pass": results, "fail": failures}
    )


def _check_l2type_coverage(
    cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # broad by design: org_districts is in the app DB, not always reachable
        # Skip (don't hard-fail) to match build_indexes._l2type_coverage, which returns None
        # and still completes. org_districts is an environmental dependency in a different DB
        # that may be persistently unreachable (separate VPC); failing closed here would wedge
        # the pipeline (step 5 completes, step 7 never passes). Distinct from the required
        # inspect baseline in _check_prod_row_counts, which DOES fail closed. The skip is
        # surfaced in details + a warning, not silently green.
        log.warning("validate.l2type.skip", check="l2Type_coverage", error=str(e))
        return ValidationCheck(
            name="l2Type_coverage",
            passed=True,
            details={"skipped": "org_districts unreachable", "error": str(e)},
        )
    with connect_new(cfg, run_date, forward=forward) as conn:
        new_cols = _columns(conn, "Voter")
    missing = sorted(v for v in distinct_l2types if v not in new_cols)
    return ValidationCheck(
        name="l2Type_coverage",
        passed=not missing,
        details={"distinct_l2types": len(distinct_l2types), "missing_columns": missing},
    )


def _check_indexes_valid(
    cfg: LoaderConfig, run_date: str, table: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    """Every index on the table is VALID on the new cluster (not just present by name).

    build_indexes builds each plain index `ON ONLY` the parent, then builds + ATTACHes a child per
    partition; the parent's partitioned index (and the partitioned PK/unique) only flips
    `indisvalid=true` once EVERY child is attached. An incomplete attach leaves the index
    present-but-INVALID — so `index_constraint_diff_clean`, which lists `pg_indexes` by name, still
    passes — yet the planner won't use it and a partitioned unique won't enforce uniqueness. Fail on
    any invalid index. New cluster only (this is about the fresh build's health, not a prod diff).
    """
    name = f"indexes_valid:{table}"
    query = (
        "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE i.indrelid = %s::regclass AND NOT i.indisvalid ORDER BY c.relname"
    )
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute(query, (f'public."{table}"',))
        invalid = [r[0] for r in cur.fetchall()]
    return ValidationCheck(name=name, passed=not invalid, details={"invalid_indexes": invalid[:20]})


# The camelCase top-level keys the app expects in DistrictStats.buckets — reuse the single
# definition from the unload shim that produces them, so the two can't drift.
_EXPECTED_BUCKET_KEYS = BUCKETS_OUTPUT_KEYS


def _check_districtstats_buckets(
    cfg: LoaderConfig, run_date: str, *, forward: tuple[str, int] | None = None
) -> ValidationCheck:
    """DistrictStats.buckets survived the mart->jsonb rename shim: non-null, and a sample row carries
    the expected camelCase top-level keys.

    `buckets` is nullable and built by the unload's `named_struct(...)`/`to_json` rename (mart's
    lowercase struct -> app camelCase). If that transform regresses it loads all-NULL or wrong-keyed
    jsonb and EVERY other check still passes (counts match, schema matches, indexes valid). This is
    the one column with a silent-failure mode, so guard it directly: fail if there's no non-null
    sample at all (all-NULL) or the sample lacks the expected camelCase keys. We do NOT require zero
    NULLs — the column is nullable, so a stray NULL district must not block a faithful load; the
    NULL count is reported for visibility. Skips cleanly if DistrictStats is absent or empty.
    """
    name = "districtstats_buckets_shape"
    with connect_new(cfg, run_date, forward=forward) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.\"DistrictStats\"')")
        row = cur.fetchone()
        if row is None or row[0] is None:
            return ValidationCheck(name=name, passed=True, details={"skipped": "DistrictStats absent"})
        cur.execute('SELECT count(*) FILTER (WHERE buckets IS NULL), count(*) FROM public."DistrictStats"')
        counts = cur.fetchone()
        null_buckets, total = (int(counts[0]), int(counts[1])) if counts else (0, 0)
        if total == 0:
            return ValidationCheck(name=name, passed=True, details={"skipped": "DistrictStats empty"})
        cur.execute('SELECT buckets FROM public."DistrictStats" WHERE buckets IS NOT NULL LIMIT 1')
        sample = cur.fetchone()
    # psycopg3 decodes jsonb to a dict; fall back to {} defensively.
    sample_keys = set((sample[0] or {}).keys()) if sample and sample[0] is not None else set()
    missing_keys = sorted(_EXPECTED_BUCKET_KEYS - sample_keys)
    return ValidationCheck(
        name=name,
        # `missing_keys` is non-empty when there's no non-null sample (all-NULL) or the sample is
        # wrong-keyed — the two rename-shim regressions. A stray NULL alone (nullable column) is fine.
        passed=not missing_keys,
        details={
            "null_buckets": null_buckets,
            "total": total,
            "sample_keys": sorted(sample_keys),
            "missing_keys": missing_keys,
        },
    )


def _to_markdown(manifest: ValidateManifest) -> str:
    lines: list[str] = [
        f"# Voter-DB Refresh Validation — {manifest.run_date}",
        "",
        f"**Status:** {'PASS' if manifest.all_passed else 'FAIL'}",
        f"**Started:** {manifest.started_at.isoformat()}",
        f"**Finished:** {manifest.finished_at.isoformat() if manifest.finished_at else '—'}",
        "",
        "## Checks",
        "",
    ]
    for c in manifest.checks:
        lines.append(f"### {c.name} — {'PASS' if c.passed else 'FAIL'}")
        lines.append("")
        for k, v in c.details.items():
            pretty = [*v[:10], "..."] if isinstance(v, list) and len(v) > 10 else v
            lines.append(f"- **{k}:** `{pretty}`")
        lines.append("")
    return "\n".join(lines)


def run(cfg: LoaderConfig, run_date: str) -> ValidateManifest:
    bind(run_date=run_date, step="validate")
    existing = read_manifest(cfg, run_date, "validate", ValidateManifest)
    if existing and existing.status == "complete":
        log.info(
            "validate.skip", reason="manifest already complete", uri=manifest_uri(cfg, run_date, "validate")
        )
        return existing

    unload = read_manifest(cfg, run_date, "unload", UnloadManifest)
    if unload is None or unload.status != "complete":
        raise RuntimeError("Step 7 requires a completed unload manifest (per-state baseline).")
    if not any(t.table == "Voter" for t in unload.tables):
        raise RuntimeError("unload manifest has no Voter table")

    started = datetime.now(UTC)
    log.info("validate.start")

    # Run all checks. An unexpected exception (new cluster unreachable, SSM connection-string
    # fetch timeout, an unguarded connect_new inside a check) must still leave a `failed` manifest
    # behind so a retry sees a known state — never propagate out of run() with nothing
    # written. The per-check functions capture their own expected failures; this catches
    # everything else.
    try:
        # One shared bastion tunnel for every new-cluster query below (mirrors build_indexes): each
        # connect_new multiplexes through `fwd`, so the whole step makes one SSH handshake instead of
        # one per check x per table. `fwd` is None with no bastion (direct/local). connect_prod is
        # left per-call (few, and there's no shared prod-tunnel helper).
        with open_new_tunnel(cfg, run_date) as fwd:
            # Query the new cluster's counts once per table; both count gates reuse them.
            count_checks: list[ValidationCheck] = []
            new_counts: dict[str, dict[str, int] | int] = {}
            for t in unload.tables:
                check, actual = _count_gate_check(cfg, run_date, t, forward=fwd)
                count_checks.append(check)
                new_counts[t.table] = actual

            checks: list[ValidationCheck] = [
                *count_checks,
                *_check_prod_row_counts(cfg, run_date, new_counts),
                *[_check_schema_diff(cfg, run_date, t.table, forward=fwd) for t in unload.tables],
                *[_check_indexes(cfg, run_date, t.table, forward=fwd) for t in unload.tables],
                *[_check_indexes_valid(cfg, run_date, t.table, forward=fwd) for t in unload.tables],
                _check_districtstats_buckets(cfg, run_date, forward=fwd),
                _check_sample_queries(cfg, run_date, forward=fwd),
                _check_index_usage(cfg, run_date, forward=fwd),
                _check_l2type_coverage(cfg, run_date, forward=fwd),
            ]
    except Exception as e:  # broad by design: persist a failed manifest, then re-raise
        log.error("validate.errored", error=str(e))
        errored = ValidateManifest(
            run_date=run_date,
            status="failed",
            started_at=started,
            finished_at=datetime.now(UTC),
            checks=[ValidationCheck(name="validate_errored", passed=False, details={"error": str(e)})],
            all_passed=False,
        )
        write_manifest(cfg, errored)
        raise

    for c in checks:
        log.info("validate.check", name=c.name, passed=c.passed)

    all_passed = all(c.passed for c in checks)
    # Write "failed" (not "complete") on a failed gate so the skip-guard does not
    # short-circuit a retry: an operator can re-run validate after fixing the data
    # without manually deleting the S3 manifest.
    manifest = ValidateManifest(
        run_date=run_date,
        status="complete" if all_passed else "failed",
        started_at=started,
        finished_at=datetime.now(UTC),
        checks=checks,
        all_passed=all_passed,
    )
    md_uri = put_artifact(cfg, run_date, "_manifest/validate.md", _to_markdown(manifest))
    log.info("validate.markdown", uri=md_uri)
    uri = write_manifest(cfg, manifest)
    log.info("validate.complete", uri=uri, all_passed=all_passed)
    return manifest
