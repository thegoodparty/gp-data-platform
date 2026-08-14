"""Win top-line reporting: accounts, activated, Pro per general-election-date bucket.

Spec: win_topline_reporting_brief.yaml (brief_id: win-topline-election-buckets),
amended 2026-07-22 after the two-reviewer pass (anchored activation, Pro evidence
layer split, y2026 held/upcoming split, presentation fixes).

Build-once-slice-many: one user-grain working-set query, all bucketing and
metric logic in pandas so re-cuts and bucket changes are free.

Restatement rule (recurring runs): users are assigned to their most recently
created campaign's bucket at run time, and activated_lifetime / pro_today are
as-of-run-date flags — historical rows restate slightly across runs. Anchored
columns (activated_at_election, pro_at_election) restate only via bucket
migration (bounded by multi-candidacy users, reported in the sanity block).

Run from the repo root:
    cd analytics && uv run python projects/win_topline_reporting/topline_report.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
import databricks_conn as dbc

# --- Parameters (edit here for future reporting rounds) ---------------------

# Buckets on bucket_date = COALESCE(general_election_date, election_date),
# inclusive on both ends. Dated candidacies outside every bucket are dropped
# and footnoted (owner decision 2026-07-22; drop magnitude 15,824 reconfirmed
# by owner 2026-07-22 after review).
BUCKETS: dict[str, tuple[date, date]] = {
    "summer_2025": (date(2025, 5, 8), date(2025, 9, 1)),
    "november_2025": (date(2025, 11, 1), date(2025, 11, 30)),
    "y2026": (date(2026, 1, 1), date(2026, 12, 31)),
}
# Buckets whose elections straddle the run date get split into held/upcoming.
SPLIT_BY_RUN_DATE = {"y2026"}
# The no-election-date bucket only counts accounts created in the reporting era.
ERA_FLOOR = date(2025, 5, 8)
# Corrupt-date guard for election dates (segmentation.md convention).
DATE_BOUNDS = (date(2020, 1, 1), date(2050, 1, 1))
INTERNAL_EMAIL_PATTERN = "%@goodparty.org"

# Election dates are bounded in SQL: corrupt values (e.g. year 202407) crash
# the sql-connector's date parsing at fetch time, so they can't reach pandas.
_LO, _HI = DATE_BOUNDS

WORKING_SET_SQL = f"""
WITH pop AS (
    SELECT
        user_id,
        LOWER(user_email) AS em,
        hubspot_id,
        CAST(user_created_at AS DATE) AS user_created_date,
        CASE WHEN general_election_date BETWEEN '{_LO}' AND '{_HI}'
             THEN general_election_date END AS general_election_date,
        CASE WHEN election_date BETWEEN '{_LO}' AND '{_HI}'
             THEN election_date END AS election_date,
        is_pro,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY campaign_created_at DESC, campaign_version_id
        ) AS rn,
        COUNT(*) OVER (PARTITION BY user_id) AS n_candidacies
    FROM goodparty_data_catalog.mart_analytics.users_win_candidacy
    WHERE is_latest_version AND NOT is_demo
        AND (user_email IS NULL OR user_email NOT ILIKE '{INTERNAL_EMAIL_PATTERN}')
),
dedup AS (
    -- d_eval: the point-in-time evaluation date, min(bucket date, today);
    -- NULL if dateless.
    SELECT *,
        LEAST(COALESCE(general_election_date, election_date), current_date())
            AS d_eval
    FROM pop WHERE rn = 1
),
base AS (
    SELECT
        user_id,
        is_activated,
        CAST(first_campaign_sent_at AS DATE) AS first_campaign_sent_date
    FROM goodparty_data_catalog.mart_analytics.users_win_base
),
stripe_subs AS (
    SELECT
        LOWER(cu.email) AS em,
        s.status,
        CAST(from_unixtime(s.start_date) AS DATE) AS sub_start,
        CAST(from_unixtime(CAST(s.ended_at AS BIGINT)) AS DATE) AS sub_end
    FROM goodparty_data_catalog.dbt.stg_airbyte_source__stripe_api_subscriptions s
    JOIN goodparty_data_catalog.dbt.stg_airbyte_source__stripe_api_customers cu
        ON s.customer = cu.id
    WHERE s.livemode AND s.status != 'incomplete_expired'
),
stripe_flags AS (
    -- Interval test in SQL (complex types don't survive the connector without
    -- pyarrow). Active-on-D iff a subscription interval covers d_eval; an
    -- open-ended interval only counts while the subscription is in good
    -- standing (excludes past_due, per review 2026-07-22).
    SELECT
        d.user_id,
        TRUE AS has_stripe,
        MAX(CASE WHEN ss.sub_start <= d.d_eval
                 AND (ss.sub_end >= d.d_eval
                      OR (ss.sub_end IS NULL AND ss.status = 'active'))
            THEN 1 ELSE 0 END) = 1 AS stripe_pro_at_d
    FROM dedup d
    JOIN stripe_subs ss ON d.em = ss.em
    GROUP BY d.user_id
),
amp AS (
    SELECT user_id, CAST(pro_upgrade_completed_at AS DATE) AS amp_upgrade_date
    FROM goodparty_data_catalog.dbt.int__amplitude_user_milestones
    WHERE pro_upgrade_completed_at IS NOT NULL
),
hs AS (
    SELECT
        id AS hubspot_id,
        COALESCE(properties_combined_pro_upgrade_date, properties_pro_upgrade_date)
            AS hs_upgrade_date
    FROM goodparty_data_catalog.dbt.snapshot__hubspot_api_companies
    WHERE dbt_valid_to IS NULL
        AND COALESCE(properties_combined_pro_upgrade_date, properties_pro_upgrade_date)
            IS NOT NULL
),
sub_flags AS (
    -- Server-side Segment events from gp-api (2026-07-24 amendment).
    -- Confirmed fires on Stripe checkout AND on admin isPro grants
    -- (paymentMethod='admin') — the only channel dating comped Pros; in-data
    -- 2025-07-07..2026-04-13 (live in code, silent since April). Canceled
    -- added 2025-09-17, code-retired 2026-07-13 (omni PR #732) — historical
    -- gate only. Both pre-filtered to <= d_eval here (unlike amp/hs, which
    -- are single-value milestones compared in pandas) so a post-D re-upgrade
    -- or cancel can't leak into the at-D judgment.
    SELECT
        d.user_id,
        MAX(CASE WHEN e.event_type = 'Account - Pro Subscription Confirmed'
                 AND CAST(e.event_time AS DATE) <= d.d_eval
            THEN CAST(e.event_time AS DATE) END) AS sub_confirmed_le_d,
        MAX(CASE WHEN e.event_type = 'Account - Pro Subscription Canceled'
                 AND CAST(e.event_time AS DATE) <= d.d_eval
            THEN CAST(e.event_time AS DATE) END) AS sub_canceled_le_d
    FROM dedup d
    JOIN goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
        ON d.user_id = TRY_CAST(e.user_id AS BIGINT)
    WHERE e.event_type IN (
        'Account - Pro Subscription Confirmed',
        'Account - Pro Subscription Canceled'
    )
    GROUP BY d.user_id
)
SELECT
    d.user_id,
    d.user_created_date,
    d.general_election_date,
    d.election_date,
    d.is_pro,
    d.n_candidacies,
    COALESCE(b.is_activated, FALSE) AS is_activated,
    b.first_campaign_sent_date,
    COALESCE(sf.has_stripe, FALSE) AS has_stripe,
    COALESCE(sf.stripe_pro_at_d, FALSE) AS stripe_pro_at_d,
    a.amp_upgrade_date,
    h.hs_upgrade_date,
    sub.sub_confirmed_le_d,
    sub.sub_canceled_le_d
FROM dedup d
LEFT JOIN base b ON d.user_id = b.user_id
LEFT JOIN stripe_flags sf ON d.user_id = sf.user_id
LEFT JOIN amp a ON d.user_id = a.user_id
LEFT JOIN hs h ON d.hubspot_id = h.hubspot_id
LEFT JOIN sub_flags sub ON d.user_id = sub.user_id
"""

# --- Metric logic (pandas) ---------------------------------------------------


def bucket_date(row: pd.Series) -> date | None:
    """COALESCE(general_election_date, election_date); bounds applied in SQL."""
    d = row["general_election_date"]
    if pd.isna(d):
        d = row["election_date"]
    return None if pd.isna(d) else pd.Timestamp(d).date()


def assign_bucket(d: date | None, created: date) -> str:
    if d is None:
        return "no_election_date" if created >= ERA_FLOOR else "pre_era_no_date"
    for name, (lo, hi) in BUCKETS.items():
        if lo <= d <= hi:
            return name
    return "outside_buckets"


def display_labels(asof: date) -> dict[str, str]:
    """Row labels carrying explicit date ranges (review: bucket names hide them)."""

    def _rng(lo: date, hi: date) -> str:
        return f"{lo:%b %-d, %Y} - {hi:%b %-d, %Y}"

    labels = {}
    for name, (lo, hi) in BUCKETS.items():
        if name in SPLIT_BY_RUN_DATE:
            labels[f"{name}_held"] = f"Elections {_rng(lo, hi)} (already held)"
            labels[f"{name}_upcoming"] = f"Elections {_rng(lo, hi)} (upcoming)"
        else:
            labels[name] = f"Elections {_rng(lo, hi)}"
    labels["no_election_date"] = f"No election date (accounts created since {ERA_FLOOR:%b %-d, %Y})"
    labels["TOTAL"] = "TOTAL (rows above)"
    return labels


def build_working_flags(df: pd.DataFrame, asof: date) -> pd.DataFrame:
    """Attach bucket assignment and all per-user metric flags."""
    df = df.copy()
    df["user_created_date"] = pd.to_datetime(df["user_created_date"]).dt.date
    df["bucket_dt"] = df.apply(bucket_date, axis=1)
    df["used_fallback"] = df["general_election_date"].isna() & df["bucket_dt"].notna()
    df["bucket"] = df.apply(lambda r: assign_bucket(r["bucket_dt"], r["user_created_date"]), axis=1)
    # Split straddling buckets into held/upcoming relative to the run date.
    for name in SPLIT_BY_RUN_DATE:
        mask = df["bucket"] == name
        held = df["bucket_dt"].apply(lambda d: d is not None and d <= asof)
        df.loc[mask & held, "bucket"] = f"{name}_held"
        df.loc[mask & ~held, "bucket"] = f"{name}_upcoming"

    d_eval = df["bucket_dt"].apply(lambda d: None if d is None else min(d, asof))

    def _last_sig_by(row_idx: int, d: date | None, cols: tuple[str, ...]) -> date | None:
        """Latest upgrade signal on or before d across the given channels.

        sub_confirmed_le_d is already <= d_eval from SQL; amp/hs are raw
        milestone dates compared here.
        """
        if d is None:
            return None
        best = None
        for col in cols:
            v = df.at[row_idx, col]
            if not pd.isna(v):
                vd = pd.Timestamp(v).date()
                if vd <= d and (best is None or vd > best):
                    best = vd
        return best

    legacy_channels = ("amp_upgrade_date", "hs_upgrade_date")
    all_channels = (*legacy_channels, "sub_confirmed_le_d")

    def _fallback(row_idx: int, d: date | None, cols: tuple[str, ...], gate: bool) -> bool:
        """Non-Stripe fallback: latest upgrade signal <= d, optionally gated by
        a cancel event after that signal (a later upgrade signal re-qualifies)."""
        if df.at[row_idx, "has_stripe"]:
            return False
        sig = _last_sig_by(row_idx, d, cols)
        if sig is None:
            return False
        if gate:
            cancel = df.at[row_idx, "sub_canceled_le_d"]
            if not pd.isna(cancel) and pd.Timestamp(cancel).date() > sig:
                return False
        return True

    df["pro_at_stripe"] = df["has_stripe"] & df["stripe_pro_at_d"] & d_eval.notna()
    df["pro_at_fallback"] = [_fallback(i, d, all_channels, gate=True) for i, d in d_eval.items()]
    df["pro_at_election"] = df["pro_at_stripe"] | df["pro_at_fallback"]
    # Prior logic (pre-2026-07-24: legacy channels, no cancel gate) kept for the
    # delta table; drop once a reporting round has restated on the new logic.
    df["pro_at_fallback_prev"] = [_fallback(i, d, legacy_channels, gate=False) for i, d in d_eval.items()]
    df["pro_at_election_prev"] = df["pro_at_stripe"] | df["pro_at_fallback_prev"]
    # Sanity #6: signal existed but a later cancel gated it off.
    df["cancel_gated"] = [
        _fallback(i, d, all_channels, gate=False) and not df.at[i, "pro_at_fallback"]
        for i, d in d_eval.items()
    ]

    fcs = pd.to_datetime(df["first_campaign_sent_date"], errors="coerce")
    df["activated_at_election"] = [
        (d is not None) and (not pd.isna(f)) and (f.date() <= d) for d, f in zip(d_eval, fcs, strict=False)
    ]
    return df


REPORT_COLUMNS = [
    "accounts",
    "activated_lifetime",
    "activated_at_election",
    "activated_at_election_pct",
    "pro_at_election",
    "pro_at_stripe",
    "pro_at_fallback",
    "pro_at_election_pct",
    "pro_today",
]

# At-election cells are structural (not observed zeros) for the dateless row.
STRUCTURAL_NA_ROW = "no_election_date"
AT_ELECTION_COLS = [
    "activated_at_election",
    "activated_at_election_pct",
    "pro_at_election",
    "pro_at_stripe",
    "pro_at_fallback",
    "pro_at_election_pct",
]


def build_report(
    df: pd.DataFrame, asof: date
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Return (report, crosstab, pro_activation, delta, sanity). df must come from build_working_flags."""
    report_buckets = [
        b
        for name in BUCKETS
        for b in ([f"{name}_held", f"{name}_upcoming"] if name in SPLIT_BY_RUN_DATE else [name])
    ] + ["no_election_date"]
    in_report = df[df["bucket"].isin(report_buckets)]

    flag_cols = [
        "is_activated",
        "activated_at_election",
        "pro_at_election",
        "pro_at_stripe",
        "pro_at_fallback",
        "is_pro",
    ]
    agg = in_report.groupby("bucket").agg(
        accounts=("user_id", "nunique"),
        **{c: (c, "sum") for c in flag_cols},
    )
    agg = agg.reindex(report_buckets, fill_value=0).astype(int)
    agg = agg.rename(columns={"is_activated": "activated_lifetime", "is_pro": "pro_today"})
    agg.loc["TOTAL"] = agg.sum()
    agg["activated_at_election_pct"] = (100 * agg["activated_at_election"] / agg["accounts"]).round(1)
    agg["pro_at_election_pct"] = (100 * agg["pro_at_election"] / agg["accounts"]).round(1)
    report = agg[REPORT_COLUMNS].astype(object)
    report.loc[STRUCTURAL_NA_ROW, AT_ELECTION_COLS] = "-"

    # Pro x activation cross-tab (at-election semantics), past/held buckets only.
    past_buckets = [b for b in report_buckets if b != "no_election_date" and not b.endswith("_upcoming")]
    rows = []
    for b in past_buckets:
        g = in_report[in_report["bucket"] == b]
        pro_and_act = int((g["pro_at_election"] & g["activated_at_election"]).sum())
        pro_not_act = int((g["pro_at_election"] & ~g["activated_at_election"]).sum())
        rows.append(
            {
                "bucket": b,
                "pro_and_activated": pro_and_act,
                "pro_not_activated": pro_not_act,
                "activated_not_pro": int((~g["pro_at_election"] & g["activated_at_election"]).sum()),
                "neither": int((~g["pro_at_election"] & ~g["activated_at_election"]).sum()),
                "pro_to_activated_pct": round(100 * pro_and_act / (pro_and_act + pro_not_act), 1)
                if pro_and_act + pro_not_act
                else None,
            }
        )
    crosstab = pd.DataFrame(rows).set_index("bucket")

    # Pro -> activated conversion per bucket, both semantics (2026-07-23 amendment,
    # Bryan's ratio). Lifetime = as-of-run-date flags; at-election = anchored flags.
    # Bases differ by construction: pro_today includes post-election churn-out and
    # new upgrades, pro_at_election is the evidence-only anchored count.
    pa_rows = []
    for b in [x for x in report_buckets if x != "no_election_date"]:
        g = in_report[in_report["bucket"] == b]
        pro_today = int(g["is_pro"].sum())
        pro_today_act = int((g["is_pro"] & g["is_activated"]).sum())
        pro_at = int(g["pro_at_election"].sum())
        pro_at_act = int((g["pro_at_election"] & g["activated_at_election"]).sum())
        pa_rows.append(
            {
                "bucket": b,
                "pro_today": pro_today,
                "pro_today_activated": pro_today_act,
                "pro_today_to_activated_pct": round(100 * pro_today_act / pro_today, 1)
                if pro_today
                else None,
                "pro_at_election": pro_at,
                "pro_at_election_activated": pro_at_act,
                "pro_at_election_to_activated_pct": round(100 * pro_at_act / pro_at, 1) if pro_at else None,
            }
        )
    pro_activation = pd.DataFrame(pa_rows).set_index("bucket")

    # Delta vs the pre-2026-07-24 dating logic (legacy channels, no cancel
    # gate), so the restatement is explicit in the round that introduces it.
    delta_rows = []
    for b in report_buckets:
        if b == STRUCTURAL_NA_ROW:
            continue
        g = in_report[in_report["bucket"] == b]
        prev, new = int(g["pro_at_election_prev"].sum()), int(g["pro_at_election"].sum())
        delta_rows.append(
            {
                "bucket": b,
                "pro_at_election_prev_logic": prev,
                "pro_at_election_new_logic": new,
                "delta": new - prev,
                "added_by_confirmed_evt": int((g["pro_at_election"] & ~g["pro_at_election_prev"]).sum()),
                "removed_by_cancel_gate": int((~g["pro_at_election"] & g["pro_at_election_prev"]).sum()),
            }
        )
    delta = pd.DataFrame(delta_rows).set_index("bucket")

    dropped = int((df["bucket"] == "outside_buckets").sum())
    pre_era = int((df["bucket"] == "pre_era_no_date").sum())
    sanity = {
        "asof": asof,
        "total_population": len(df),
        "multi_candidacy_users": int((df["n_candidacies"] > 1).sum()),
        "dropped_outside_buckets": dropped,
        "dropped_pro_today": int(df.loc[df["bucket"] == "outside_buckets", "is_pro"].sum()),
        "pre_era_no_date_excluded": pre_era,
        "fallback_per_bucket": in_report.groupby("bucket")["used_fallback"].sum().astype(int).to_dict(),
        "pro_evidence_overlap": {
            "stripe_matched": int(df["has_stripe"].sum()),
            "amp_signal": int(df["amp_upgrade_date"].notna().sum()),
            "hs_signal": int(df["hs_upgrade_date"].notna().sum()),
            "sub_confirmed_signal": int(df["sub_confirmed_le_d"].notna().sum()),
            "sub_confirmed_only": int(
                (
                    df["sub_confirmed_le_d"].notna()
                    & ~df["has_stripe"]
                    & df["amp_upgrade_date"].isna()
                    & df["hs_upgrade_date"].isna()
                ).sum()
            ),
        },
        "cancel_gated_users": int(df["cancel_gated"].sum()),
        "reconciliation_ok": int(report.loc[report.index != "TOTAL", "accounts"].sum()) + dropped + pre_era
        == len(df),
    }
    return report, crosstab, pro_activation, delta, sanity


def csv_footer(sanity: dict) -> list[str]:
    """Universe statement + footnotes that must travel with the table."""
    return [
        "",
        f"# Run date: {sanity['asof']}. Universe: latest-version non-demo Win "
        "accounts excluding @goodparty.org (internal-account proxy).",
        f"# Not shown: {sanity['dropped_outside_buckets']:,} accounts with an "
        "election date outside every window above (mostly pre-2025 cycles; "
        f"includes {sanity['dropped_pro_today']:,} current Pro users) and "
        f"{sanity['pre_era_no_date_excluded']:,} dateless accounts created "
        "before the era floor. Column sums are NOT org totals.",
        "# accounts = users whose ELECTION falls in the window (not accounts "
        "created in that window). The no-election-date row is instead keyed "
        "by account-creation date.",
        "# activated_at_election / pro_at_election are anchored at "
        "min(election date, run date). pro_at_election is evidence-only: "
        "Stripe subscription interval covering the date (pro_at_stripe), "
        "else the latest upgrade signal on or before it for users with no "
        "Stripe history (pro_at_fallback; incl. admin/comped grants via the "
        "'Account - Pro Subscription Confirmed' event, and gated by the "
        "'Account - Pro Subscription Canceled' event where one exists). "
        "~42% of today's Pro users still have no timing evidence "
        "(pre-2025-06-27 grants; grants after 2026-04-13 while the Confirmed "
        "event is silent in-data; billing-email mismatch), so "
        "pro_at_election undercounts; pro_today is the as-of-run-date "
        "reference. Restated 2026-07-24: not 1:1 comparable with earlier "
        "runs (see the restatement table).",
        "# '-' = not defined for that row (no election date), not a measured "
        "zero. The dateless row's zero activated/Pro is genuine: these are "
        "signups that never set campaign details.",
        "# Rows restate slightly across runs: lifetime/today flags are "
        "as-of-run-date, and multi-candidacy users are re-assigned to their "
        "most recent campaign's bucket.",
    ]


def main() -> None:
    asof = date.today()
    print(f"Pulling working set (as of {asof}) ...", flush=True)
    raw = dbc.run_query(WORKING_SET_SQL)
    print(f"Working set: {len(raw):,} users\n")

    df = build_working_flags(raw, asof)
    report, crosstab, pro_activation, delta, sanity = build_report(df, asof)
    labels = display_labels(asof)

    print("=== Win top-line report, by general-election-date bucket ===")
    print(report.rename(index=labels).to_string())
    print("\n=== Pro x activation cross-tab (at-election, held buckets) ===")
    print(crosstab.rename(index=labels).to_string())
    print("\n=== Pro -> activated conversion (lifetime + at-election) ===")
    print(pro_activation.rename(index=labels).to_string())
    print("\n=== pro_at_election: 2026-07-24 dating-logic restatement ===")
    print(delta.rename(index=labels).to_string())
    print("\n=== Sanity checks (brief execution_notes 1-5) ===")
    for k, v in sanity.items():
        print(f"{k}: {v}")

    # Output is stdout-only (owner decision 2026-07-23: CSV files were never
    # viewed or committed). The footnotes must travel with any table that
    # leaves this terminal — paste them alongside.
    print("\n=== Footnotes (paste with any shared table) ===")
    print("\n".join(line for line in csv_footer(sanity) if line))
    print(
        "# Pro -> activated table: pro_today / is_activated are as-of-run-date"
        " (lifetime) flags; at-election columns are anchored at min(election"
        " date, run date) and evidence-only. Bases differ by construction; for"
        " upcoming elections the two semantics converge on 'today'. Activated"
        " and Pro are OVERLAPPING flags, not mutually exclusive segments."
    )


if __name__ == "__main__":
    main()
