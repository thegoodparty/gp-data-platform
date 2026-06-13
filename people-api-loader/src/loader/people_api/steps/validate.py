"""Step 7 — validate the new cluster before handoff (ClickUp DATA-1911).

Six checks against the unified public."Voter" table (all must pass):
1. row_counts_match_databricks — per-state count (GROUP BY "State") within ±10% of
   the unload baseline (load integrity: did COPY load everything that was unloaded).
2. prod_row_counts_within_tolerance — per-state count within ±10% of the inspect-prod
   baseline (sanity: refresh magnitude vs the current Present cluster). Fails closed if
   inspect-prod hasn't run for this run_date — run it first.
3. schema_diff_clean — new Voter columns equal prod Voter columns.
4. index_constraint_diff_clean — every prod index present on new.
5. sample_queries_pass — voterFile.util.ts-shaped queries return without error.
6. l2Type_coverage — every distinct org_districts l2Type maps to a column.

Writes validate.json + a Markdown companion. all_passed=False blocks handoff
(cli.py exits non-zero).
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, resolve_writer_endpoint
from loader.people_api.manifests import (
    InspectManifest,
    UnloadManifest,
    ValidateManifest,
    ValidationCheck,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)

log = get_logger(__name__)

# Per-state row-count gate: within ±10% of a baseline (decided 2026-06-09).
_ROW_COUNT_TOLERANCE = 0.10


def _new_voter_counts_by_state(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> dict[str, int]:
    """Per-state Voter row counts on the new cluster (queried once, reused by both gates)."""
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute('SELECT "State", count(*) FROM public."Voter" GROUP BY "State"')
        # Drop a NULL-State group (the partition key is NOT NULL, so this is defensive,
        # matching inspect_prod): a None key would break JSON-keyed manifest output.
        return {row[0]: int(row[1]) for row in cur.fetchall() if row[0] is not None}


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
        low = expected_count * (1 - _ROW_COUNT_TOLERANCE)
        high = expected_count * (1 + _ROW_COUNT_TOLERANCE)
        if not (low <= actual <= high):
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


def _check_prod_row_counts(
    cfg: LoaderConfig, run_date: str, actual_by_state: dict[str, int]
) -> ValidationCheck:
    """New-vs-prod ±10% gate on Voter, using inspect-prod's Voter per-state baseline.

    Distinct from the unload gate (load integrity): this checks the refresh is roughly
    the same magnitude as the current Present cluster. Fails closed (like the other
    prod-dependent checks) when the inspect baseline is missing — returning passed=True
    there would let an all-pass run cache a `complete` manifest that permanently bypasses
    this gate on retry. The fix is to run inspect-prod for the same run_date.
    """
    inspect = read_manifest(cfg, run_date, "inspect", InspectManifest)
    if inspect is None or inspect.status != "complete":
        log.error("validate.prod_baseline_missing", reason="no completed inspect manifest")
        return ValidationCheck(
            name="prod_row_counts_within_tolerance",
            passed=False,
            details={"error": "no completed inspect manifest for this run_date"},
        )
    voter = next((t for t in inspect.tables if t.table == "Voter"), None)
    if voter is None or not voter.per_state_row_counts:
        log.error("validate.prod_baseline_missing", reason="no Voter per-state baseline")
        return ValidationCheck(
            name="prod_row_counts_within_tolerance",
            passed=False,
            details={"error": "inspect manifest has no Voter per-state baseline"},
        )
    # Don't flag states new to the cluster but absent from the prod snapshot — that's a
    # legitimate geographic expansion, not drift (the unload gate already covers integrity).
    return _compare_counts(
        "prod_row_counts_within_tolerance",
        actual_by_state,
        voter.per_state_row_counts,
        flag_unexpected=False,
    )


def _voter_columns(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='Voter'"
        )
        return {r[0] for r in cur.fetchall()}


def _check_schema_diff(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    try:
        with connect_prod(cfg) as prod_conn:
            prod_cols = _voter_columns(prod_conn)
    except Exception as e:  # broad by design: prod may be unreachable; record as a failed check
        log.error("validate.prod_unreachable", check="schema_diff_clean", error=str(e))
        return ValidationCheck(name="schema_diff_clean", passed=False, details={"error_reading_prod": str(e)})
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        new_cols = _voter_columns(conn)
    missing_from_new = prod_cols - new_cols
    extra_in_new = new_cols - prod_cols
    return ValidationCheck(
        name="schema_diff_clean",
        passed=not missing_from_new and not extra_in_new,
        details={
            "prod_cols": len(prod_cols),
            "new_cols": len(new_cols),
            "missing_from_new": sorted(missing_from_new)[:20],
            "extra_in_new": sorted(extra_in_new)[:20],
        },
    )


def _check_indexes(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    query = (
        "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename='Voter' ORDER BY indexname"
    )
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute(query)
            prod_idx = {r[0] for r in cur.fetchall()}
    except Exception as e:  # broad by design: prod may be unreachable; record as a failed check
        log.error("validate.prod_unreachable", check="index_constraint_diff_clean", error=str(e))
        return ValidationCheck(
            name="index_constraint_diff_clean", passed=False, details={"error_reading_prod": str(e)}
        )
    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(query)
        new_idx = {r[0] for r in cur.fetchall()}
    missing = sorted(prod_idx - new_idx)
    return ValidationCheck(
        name="index_constraint_diff_clean",
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


def _check_sample_queries(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    results: dict[str, str] = {}
    failures: dict[str, str] = {}
    with connect_new(cfg, run_date, writer_endpoint) as conn:
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


def _check_l2type_coverage(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute('SELECT DISTINCT "l2Type" FROM public.org_districts WHERE "l2Type" IS NOT NULL')
            distinct_l2types = [r[0] for r in cur.fetchall() if r[0]]
    except Exception as e:  # broad by design: org_districts is in the app DB, not always reachable
        log.error("validate.prod_unreachable", check="l2Type_coverage", error=str(e))
        return ValidationCheck(
            name="l2Type_coverage", passed=False, details={"error_reading_org_districts": str(e)}
        )
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        new_cols = _voter_columns(conn)
    missing = sorted(v for v in distinct_l2types if v not in new_cols)
    return ValidationCheck(
        name="l2Type_coverage",
        passed=not missing,
        details={"distinct_l2types": len(distinct_l2types), "missing_columns": missing},
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

    writer_endpoint = resolve_writer_endpoint(cfg, run_date)
    started = datetime.now(UTC)
    log.info("validate.start")

    # Run all checks. An unexpected exception (new cluster unreachable, Secrets Manager
    # timeout, an unguarded connect_new inside a check) must still leave a `failed` manifest
    # behind so a retry sees a known state — never propagate out of run() with nothing
    # written. The per-check functions capture their own expected failures; this catches
    # everything else.
    try:
        # Query the new cluster's per-state Voter counts once; both count gates reuse it.
        new_counts = _new_voter_counts_by_state(cfg, run_date, writer_endpoint)
        checks: list[ValidationCheck] = [
            _compare_counts("row_counts_match_databricks", new_counts, unload.per_state_row_counts),
            _check_prod_row_counts(cfg, run_date, new_counts),
            _check_schema_diff(cfg, run_date, writer_endpoint),
            _check_indexes(cfg, run_date, writer_endpoint),
            _check_sample_queries(cfg, run_date, writer_endpoint),
            _check_l2type_coverage(cfg, run_date, writer_endpoint),
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
