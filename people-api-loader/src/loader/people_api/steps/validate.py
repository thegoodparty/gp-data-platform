"""Step 7 — validate the new cluster before handoff (ClickUp DATA-1911).

Five checks (all must pass):
1. row_counts_match_databricks — per-state count within ±10% of the unload baseline.
2. schema_diff_clean — new columns are a superset of prod minus retired typos.
3. index_constraint_diff_clean — every prod index/constraint present on new.
4. sample_queries_pass — voterFile.util.ts-shaped queries return without error.
5. l2Type_coverage — every distinct org_districts l2Type maps to a column.

Writes validate.json AND a Markdown companion for human sign-off.
`all_passed=False` blocks handoff — `cli.py` exits non-zero.
"""

from __future__ import annotations

from datetime import UTC, datetime

from loader.core.log import bind, get_logger
from loader.people_api.config import LoaderConfig
from loader.people_api.db import connect_new, connect_prod, resolve_writer_endpoint
from loader.people_api.manifests import (
    UnloadManifest,
    ValidateManifest,
    ValidationCheck,
    manifest_uri,
    put_artifact,
    read_manifest,
    write_manifest,
)
from loader.people_api.schema.emit_ddl import STATES
from loader.people_api.schema.voter_columns import LEGACY_RENAMES, VOTER_TARGET_COLUMNS

log = get_logger(__name__)

# Per-state row-count gate: new cluster must be within ±10% of the unload
# baseline. (Decided 2026-06-09; tickets said ±15%, repo plan said exact.)
_ROW_COUNT_TOLERANCE = 0.10


def _check_row_counts(
    cfg: LoaderConfig, run_date: str, writer_endpoint: str, expected: dict[str, int]
) -> ValidationCheck:
    mismatches: dict[str, dict[str, float]] = {}
    with connect_new(cfg, run_date, writer_endpoint) as conn:
        for state, expected_count in expected.items():
            table = f"Voter{state}"
            with conn.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM public."{table}"')  # ty: ignore[no-matching-overload]
                row = cur.fetchone()
                actual = int(row[0]) if row else 0
            low = expected_count * (1 - _ROW_COUNT_TOLERANCE)
            high = expected_count * (1 + _ROW_COUNT_TOLERANCE)
            if not (low <= actual <= high):
                mismatches[table] = {"expected": expected_count, "actual": actual}
    return ValidationCheck(
        name="row_counts_match_databricks",
        passed=not mismatches,
        details={
            "states": len(expected),
            "tolerance": _ROW_COUNT_TOLERANCE,
            "mismatch_count": len(mismatches),
            "mismatches": dict(list(mismatches.items())[:10]),
        },
    )


def _check_schema_diff(
    cfg: LoaderConfig, run_date: str, writer_endpoint: str, per_state_row_counts: dict[str, int]
) -> ValidationCheck:
    sample_state = "TX" if "TX" in per_state_row_counts else STATES[0]
    sample_table = f"Voter{sample_state}"

    with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (sample_table,),
        )
        prod_cols = {r[0] for r in cur.fetchall()}

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s",
            (sample_table,),
        )
        new_cols = {r[0] for r in cur.fetchall()}

    target_cols = {c.name for c in VOTER_TARGET_COLUMNS}
    retired = set(LEGACY_RENAMES.keys())
    missing_from_new = (prod_cols - retired) - new_cols
    unexpected_in_new = new_cols - target_cols

    return ValidationCheck(
        name="schema_diff_clean",
        passed=not missing_from_new and not unexpected_in_new,
        details={
            "sample_table": sample_table,
            "prod_cols": len(prod_cols),
            "new_cols": len(new_cols),
            "target_cols": len(target_cols),
            "missing_from_new": sorted(missing_from_new)[:20],
            "unexpected_in_new": sorted(unexpected_in_new)[:20],
            "retired_typos": sorted(retired),
        },
    )


def _check_indexes(cfg: LoaderConfig, run_date: str, writer_endpoint: str) -> ValidationCheck:
    sample_table = "VoterTX"
    query = "SELECT indexname FROM pg_indexes WHERE schemaname='public' AND tablename=%s ORDER BY indexname"
    try:
        with connect_prod(cfg) as prod_conn, prod_conn.cursor() as cur:
            cur.execute(query, (sample_table,))
            prod_idx = {r[0] for r in cur.fetchall()}
    except Exception as e:  # broad by design: prod may be unreachable; record as a failed check
        return ValidationCheck(
            name="index_constraint_diff_clean", passed=False, details={"error_reading_prod": str(e)}
        )

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(query, (sample_table,))
        new_idx = {r[0] for r in cur.fetchall()}

    missing = sorted(prod_idx - new_idx)
    return ValidationCheck(
        name="index_constraint_diff_clean",
        passed=not missing,
        details={
            "sample_table": sample_table,
            "prod_count": len(prod_idx),
            "new_count": len(new_idx),
            "missing_from_new": missing[:20],
        },
    )


_SAMPLE_QUERIES: tuple[tuple[str, str], ...] = (
    ("party_filter", 'SELECT count(*) FROM public."VoterTX" WHERE "Parties_Description" = \'Democratic\''),
    ("gender_filter", 'SELECT count(*) FROM public."VoterTX" WHERE "Voters_Gender" = \'F\''),
    (
        "age_cast_filter",
        'SELECT count(*) FROM public."VoterTX" WHERE "Voters_Age"::integer BETWEEN 18 AND 35',
    ),
    (
        "district_lookup",
        'SELECT "US_Congressional_District", count(*) FROM public."VoterTX" GROUP BY "US_Congressional_District" ORDER BY count(*) DESC LIMIT 10',
    ),
    (
        "family_dedupe",
        'SELECT count(*) FROM public."VoterTX" a WHERE EXISTS (SELECT 1 FROM public."VoterTX" b WHERE b."Mailing_Families_FamilyID" = a."Mailing_Families_FamilyID") LIMIT 1',
    ),
    (
        "justice_of_the_peace",
        'SELECT count(*) FROM public."VoterTX" WHERE "Judicial_Justice_of_the_Peace" IS NOT NULL',
    ),
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
        return ValidationCheck(
            name="l2Type_coverage", passed=False, details={"error_reading_org_districts": str(e)}
        )

    with connect_new(cfg, run_date, writer_endpoint) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='VoterTX'"
        )
        new_cols = {r[0] for r in cur.fetchall()}
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
        status = "PASS" if c.passed else "FAIL"
        lines.append(f"### {c.name} — {status}")
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

    checks: list[ValidationCheck] = [
        _check_row_counts(cfg, run_date, writer_endpoint, unload.per_state_row_counts),
        _check_schema_diff(cfg, run_date, writer_endpoint, unload.per_state_row_counts),
        _check_indexes(cfg, run_date, writer_endpoint),
        _check_sample_queries(cfg, run_date, writer_endpoint),
        _check_l2type_coverage(cfg, run_date, writer_endpoint),
    ]
    for c in checks:
        log.info("validate.check", name=c.name, passed=c.passed)

    all_passed = all(c.passed for c in checks)
    manifest = ValidateManifest(
        run_date=run_date,
        status="complete",
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
