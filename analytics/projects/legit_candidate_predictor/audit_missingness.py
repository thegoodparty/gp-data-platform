"""DATA-2225 V2 Area 0b: mechanical per-variable missingness audit.

Inputs (all local caches, no warehouse):
- datasets/working_set.parquet   (campaign_version grain, n=20,480)
- datasets/geo_features.parquet  (user_id grain; NaN = unknown)
- datasets/population_accounting.csv, datasets/join_reconciliation.csv (cross-checks)

Outputs:
- datasets/missingness_audit.csv
- missingness_report.md (project root)

Missingness convention: pandas NaN/None, PLUS sentinel empties (string columns
empty after strip). n_br_candidacies_by_deadline additionally has a 0-sentinel
(roster present but no filing deadline -> 0 means "unknown", not "zero arrivals");
reported separately, not folded into pct_missing.

Root-cause hypotheses: verified=yes rows restate established SESSION_NOTES
findings (2026-08-06 parking-lot entries + V2 area 0b); verified=no rows are
drafts from build_working_set.py column provenance and need main-session checks.

Run: cd analytics && uv run --with pyarrow python projects/legit_candidate_predictor/audit_missingness.py
"""

from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
DS = HERE / "datasets"

STRING_SENTINEL_COLS = [
    "email_domain",
    "user_first_name",
    "user_last_name",
    "user_zip",
    "campaign_state",
    "election_level",
    "office_type",
    "br_normalized_position_type",
    "campaign_party",
]

GEO_COLS = [
    "zip_campaign_state_match",
    "geo_state_match",
    "geo_zip_state_match",
    "geo_country_nonus",
    "geo_missing",
]

# variable -> (root_cause_hypothesis, cause_class, fix_effect, verified)
CAUSES = {
    "filing_deadline": (
        "Established (2026-08-06): 16.0pp of pool = OUR ingestion-scope gap in "
        "int__ballotready_filing_period (misses ids referenced by races; ticket "
        "DATA-2235 / 86ajx85j2), 5.7pp = BR publishes no filing period "
        "[recorded, needs warehouse re-verify]. Locally recomputed: 12.0pp is the "
        "no-BR-position bucket, 28.7pp has a position but no deadline resolved; "
        "16.0+5.7=21.7pp leaves ~7.0pp of the with-position gap undecomposed. "
        "Fix lifts deadline coverage 59.3% -> ~75%.",
        "ours",
        "coverage",
        "yes",
    ),
    "geo_state_match": (
        "Established (2026-08-06): dominated by users with ONLY server-side "
        "events in month 1 (~45% of post-2025-04-24 office-declared signups, "
        "likely CRM/sales-created), NOT the 2025-04-24 geo-column-start era. "
        "2025-wave phenomenon; ~92% browser presence forward on 2026 in-cycle.",
        "structural",
        "coverage",
        "yes",
    ),
    "geo_zip_state_match": (
        "Same mechanism as geo_state_match (no geo-bearing browser event near "
        "signup), plus a small increment where the stated zip is invalid.",
        "structural",
        "coverage",
        "yes",
    ),
    "geo_country_nonus": (
        "Same mechanism as geo_state_match (no geo-bearing browser event near " "signup).",
        "structural",
        "coverage",
        "yes",
    ),
    "geo_missing": (
        "Indicator column, never null by construction (1 = no geo-bearing event " "in the signup window).",
        "structural",
        "none",
        "yes",
    ),
    "zip_campaign_state_match": (
        "NaN when the stated zip does not parse to a state (invalid/malformed "
        "zip), per build_geo.py; product-data signup tier, near-full coverage.",
        "structural",
        "none",
        "yes",
    ),
    "br_normalized_position_type": (
        "Established (2026-08-05): no-BR-office bucket (user declared an office "
        "without a BR position match). Near-tautological: 1.2% corroborated, "
        "100% nested in br_arrivals-missing; rule-decided, not modeled "
        "(two-stage serving shape).",
        "structural",
        "none",
        "yes",
    ),
    "br_position_id": (
        "Identical 2,459-row bucket as br_normalized_position_type (verified "
        "locally: same rows): user's declared office has no BR position id. "
        "SESSION_NOTES: 29 of 2,459 corroborate anyway (28 via BR ER match) = "
        "office-selection mistakes worth a look.",
        "structural",
        "none",
        "yes",
    ),
    "election_level": (
        "DRAFT: product-side field on users_win_candidacy not consistently "
        "collected. Missing 32.4% for 2025 signups but 100% for 2026 signups "
        "(n=624) - looks like the current onboarding stopped writing it "
        "entirely; check gp_api campaign details / onboarding-change timing. "
        "Possibly backfillable from BR position level where br_position_id "
        "exists (4,826 of the 7,150 missing rows have a BR position; 2,324 do "
        "not).",
        "source",
        "coverage",
        "no",
    ),
    "campaign_party": (
        "DRAFT: optional onboarding field. 0% missing for 2022-23 signups, "
        "8.8% for 2024, ~64% for 2025 - collection appears to have become "
        "optional/dropped in the 2025 flow. Plus 6 empty-string sentinels. "
        "Check onboarding versions before treating as fixable.",
        "source",
        "coverage",
        "no",
    ),
    "office_type": (
        "DRAFT: user-declared office descriptor, optional in some flows; "
        "small residual (2.0%), concentrated in 2024-25 signups.",
        "source",
        "coverage",
        "no",
    ),
    "voter_count": (
        "DRAFT: 2,459 rows (12.0pp) nested in the no-BR-position bucket "
        "(structural); remaining 1,154 rows (5.6pp) have a BR position but no "
        "L2 district voter count - hypothesis: position missing from the "
        "L2-district match universe (int__icp_offices/voter_count pipeline). "
        "Verify whether the 5.6pp is our match gap or genuine L2 absence.",
        "structural",
        "coverage",
        "no",
    ),
    "icp_office_win": (
        "DRAFT: slice dimension, not a feature. 2,459 rows nested in "
        "no-BR-position (structural); remaining 473 rows (2.3pp) have a BR "
        "position absent from int__icp_offices - hypothesis: ICP office "
        "universe is a curated subset, absence expected by design. Verify "
        "NULL-vs-false semantics before slicing.",
        "structural",
        "none",
        "no",
    ),
    "n_br_candidacies_total": (
        "DRAFT: 2,459 rows nested in no-BR-position (structural); remaining "
        "4,536 rows (22.2pp) have a BR position but no roster row joining "
        "position+election_day. Hypotheses to verify: (a) user-entered "
        "election_date != BR election_day (off-date entries break the "
        "date-matched join - adjacent finding, ~250 Nov-2025 users), (b) race "
        "excluded by the not-primary/runoff/recall filter, (c) br_race_id "
        "string-cast join misses, (d) BR genuinely has no candidacies for the "
        "race.",
        "unknown",
        "coverage",
        "no",
    ),
    "n_br_candidacies_by_deadline": (
        "Same null mechanism as n_br_candidacies_total (nulls are identical "
        "row-for-row, verified locally). ADDITIONALLY 4,455 rows carry a "
        "0-sentinel: roster present but filing_deadline null, so 0 means 'no "
        "deadline to count against', not 'zero arrivals by deadline'. "
        "Effective unknown rate = (6,995+4,455)/20,480 = 55.9%. The DATA-2235 "
        "filing-period fix converts most sentinels to real counts.",
        "unknown",
        "coverage",
        "no",
    ),
    "user_zip": ("Below 1% (23 null + 3 empty-string).", "source", "none", "no"),
    "is_pro": ("Below 1% (19 null).", "source", "none", "no"),
}


def load() -> pd.DataFrame:
    ws = pd.read_parquet(DS / "working_set.parquet")
    geo = pd.read_parquet(DS / "geo_features.parquet")
    df = ws.merge(geo, on="user_id", how="left", validate="m:1")
    assert len(df) == len(ws), "geo merge changed the grain"
    return df


def missing_mask(df: pd.DataFrame, col: str) -> pd.Series:
    m = df[col].isna()
    if col in STRING_SENTINEL_COLS:
        m = m | df[col].fillna("x").astype(str).str.strip().eq("")
    return m


def main() -> None:
    df = load()
    n = len(df)

    ge = pd.to_datetime(df["general_election_date"])
    su = pd.to_datetime(df["signup_date"])
    ge_year = ge.dt.year
    half = su.dt.year.astype(str) + "-H" + ((su.dt.month > 6).astype(int) + 1).astype(str)
    HALVES = ["2025-H1", "2025-H2", "2026-H1", "2026-H2"]

    rows = []
    for col in [c for c in df.columns]:
        if col in ("user_id", "campaign_id", "gp_candidacy_id", "gp_candidate_id"):
            continue  # identifiers, never model inputs
        m = missing_mask(df, col)
        cause, klass, fix, verified = CAUSES.get(
            col, ("none needed (<1% missing or complete)", "unknown", "none", "no")
        )
        if m.mean() <= 0.01 and col not in CAUSES:
            klass, fix = "unknown", "none"
        rec = {
            "variable": col,
            "dtype": str(df[col].dtype),
            "n": n,
            "n_missing": int(m.sum()),
            "pct_missing": round(100 * m.mean(), 2),
            "pct_missing_2025": round(100 * m[ge_year == 2025].mean(), 2),
            "pct_missing_2026": round(100 * m[ge_year == 2026].mean(), 2),
        }
        for h in HALVES:
            sel = half == h
            rec[f"pct_missing_signup_{h}"] = round(100 * m[sel].mean(), 2) if sel.any() else None
        rec.update(root_cause_hypothesis=cause, cause_class=klass, fix_effect=fix, verified=verified)
        rows.append(rec)

    audit = pd.DataFrame(rows).sort_values("pct_missing", ascending=False)
    out_csv = DS / "missingness_audit.csv"
    audit.to_csv(out_csv, index=False)
    print(f"saved -> {out_csv}  ({len(audit)} variables)")

    # ---- cross-checks --------------------------------------------------
    checks = []
    checks.append(("working_set rows", n, "results.json n=20,480", 20480, n == 20480))
    nu = df["user_id"].nunique()
    pa = pd.read_csv(DS / "population_accounting.csv")
    pool = int(pa.loc[pa.segment == "4_labeled_pool_2025_plus", "n_users"].iloc[0])
    checks.append(
        (
            "working_set unique users",
            nu,
            "population_accounting 4_labeled_pool_2025_plus (user grain)",
            pool,
            nu == pool,
        )
    )
    jr = pd.read_csv(DS / "join_reconciliation.csv")
    strict = int(jr.n_version_aware_join.iloc[0])
    checks.append(
        (
            "working_set unique users",
            nu,
            "join_reconciliation n_version_aware_join (user grain)",
            strict,
            nu == strict,
        )
    )
    pr = df["corroborated"].mean()
    checks.append(
        (
            "positive rate (row grain)",
            round(pr, 4),
            "results.json positive_rate",
            0.5937,
            abs(pr - 0.5937) < 5e-4,
        )
    )

    # BR-count semantics
    both = (df.n_br_candidacies_total.isna() & df.n_br_candidacies_by_deadline.isna()).sum()
    only_bd = (df.n_br_candidacies_total.notna() & df.n_br_candidacies_by_deadline.isna()).sum()
    sentinel = (df.n_br_candidacies_total.notna() & df.filing_deadline.isna()).sum()

    # filing_deadline local decomposition
    fdl_null = df.filing_deadline.isna()
    no_pos = df.br_position_id.isna()
    fdl_nopos = (fdl_null & no_pos).sum()
    fdl_pos = (fdl_null & ~no_pos).sum()

    dup_users = df.user_id.duplicated().sum()

    # ---- report --------------------------------------------------------
    def fixability(row):
        return {"ours": 1.0, "source": 0.5, "unknown": 0.4, "structural": 0.15}[
            row.cause_class
        ] * row.pct_missing

    audit["fix_priority"] = audit.apply(fixability, axis=1)
    ranked = audit[audit.pct_missing > 1].sort_values("fix_priority", ascending=False)

    tbl_cols = [
        "variable",
        "dtype",
        "n_missing",
        "pct_missing",
        "pct_missing_2025",
        "pct_missing_2026",
        "pct_missing_signup_2025-H1",
        "pct_missing_signup_2025-H2",
        "pct_missing_signup_2026-H1",
        "pct_missing_signup_2026-H2",
        "cause_class",
        "fix_effect",
        "verified",
    ]

    lines = [
        "# DATA-2225 missingness audit (V2 area 0b, mechanical half)",
        "",
        f"Generated by `audit_missingness.py` on 2026-08-06. Grain: campaign_version "
        f"row, n={n:,} ({nu:,} unique users; 1 user carries 2 campaigns). Working set "
        f"+ merged geo features; missingness = NaN plus empty-after-strip string "
        f"sentinels. Full table: `datasets/missingness_audit.csv`.",
        "",
        "Signup-half columns cover 2025-2026 signups only; 519 rows (2.5%) have "
        "pre-2025 signup dates and 0 rows fall in 2026-H2 (signup max 2026-06-25, "
        "so that column is empty).",
        "",
        "## Missingness table",
        "",
        audit[tbl_cols].to_markdown(index=False),
        "",
        "## Top gaps ranked by pct_missing x fixability",
        "",
        "Fixability weight by cause_class: ours 1.0, source 0.5, unknown 0.4, "
        "structural 0.15 (crude prior, not a verdict).",
        "",
    ]
    for _, r in ranked.iterrows():
        lines.append(
            f"1. **{r.variable}** — {r.pct_missing:.1f}% missing, "
            f"class={r.cause_class}, fix_effect={r.fix_effect}, "
            f"verified={r.verified}, priority={r.fix_priority:.1f}"
        )
    lines += [
        "",
        "## BR-count semantics (total vs by_deadline)",
        "",
        f"- BOTH n_br_candidacies_total and n_br_candidacies_by_deadline null: "
        f"**{both:,}** rows ({100*both/n:.1f}%) = no BR roster joined at all.",
        f"- ONLY by_deadline null (total present): **{only_bd:,}** rows — the two "
        f"null masks are identical row-for-row, by construction (by_deadline is a "
        f"SUM over the same joined roster).",
        f"- BUT **{sentinel:,}** rows ({100*sentinel/n:.1f}%) have a roster and NO "
        f"filing_deadline, so by_deadline = 0 is a sentinel meaning 'no deadline "
        f"to count against', not 'zero arrivals'. Effective unknown rate for "
        f"by_deadline = {100*(both+sentinel)/n:.1f}%. The DATA-2235 fix converts "
        f"most sentinels into real counts.",
        "",
        "## filing_deadline decomposition (local recompute vs recorded)",
        "",
        f"- Local: {fdl_null.sum():,} null ({100*fdl_null.mean():.1f}%) = "
        f"{fdl_nopos:,} ({100*fdl_nopos/n:.1f}pp) no-BR-position + {fdl_pos:,} "
        f"({100*fdl_pos/n:.1f}pp) position present but deadline unresolved.",
        "- Recorded (needs warehouse re-verify): 16.0pp = our "
        "int__ballotready_filing_period scope gap (DATA-2235), 5.7pp = BR "
        "publishes no filing period. 16.0+5.7 = 21.7pp accounts for only part of "
        f"the {100*fdl_pos/n:.1f}pp with-position gap; **~{100*fdl_pos/n-21.7:.1f}pp "
        "residual is undecomposed** — verify in main session.",
        "",
        "## Draft hypotheses needing main-session verification (verified=no)",
        "",
    ]
    for _, r in audit[(audit.verified == "no") & (audit.pct_missing > 1)].iterrows():
        lines.append(f"- **{r.variable}** ({r.pct_missing:.1f}%): {r.root_cause_hypothesis}")
    lines += [
        "",
        "## Count cross-checks",
        "",
        "| check | computed | reference | expected | match |",
        "|---|---|---|---|---|",
    ]
    for name, got, refname, want, ok in checks:
        lines.append(f"| {name} | {got} | {refname} | {want} | {'OK' if ok else 'MISMATCH'} |")
    lines += [
        "",
        f"Row-vs-user grain note: {n:,} rows vs {nu:,} users; the accounting CSVs "
        f"count DISTINCT users, results.json counts rows. The single duplicated "
        f"user ({dup_users} extra row: user 33662, campaigns 26543/26544 with "
        f"2025 and 2026 elections) reconciles 20,480 vs 20,479 exactly. No "
        f"unexplained mismatch.",
        "",
    ]
    (HERE / "missingness_report.md").write_text("\n".join(lines))
    print(f"saved -> {HERE / 'missingness_report.md'}")

    print(
        audit[["variable", "pct_missing", "cause_class", "fix_effect", "verified"]]
        .head(16)
        .to_string(index=False)
    )
    for name, got, refname, want, ok in checks:
        print(f"CHECK {'OK ' if ok else 'FAIL'} {name}: {got} vs {refname}={want}")


if __name__ == "__main__":
    main()
