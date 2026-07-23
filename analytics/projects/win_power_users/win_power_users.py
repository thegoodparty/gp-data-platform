"""Working-set builder + analysis helpers for the Win power-user profile (DATA-2153).

Build-once-slice-many (analytics-process methodology): one candidacy-grain DataFrame
carrying the two power-user flags and every cross-cut dimension, then slice in pandas.

Grain: one row per gp_candidacy_id in the scoped base
(2025-2026 generals, voter_count >= 1000, is_latest_version AND NOT is_demo).

Power user is a USAGE cohort, deliberately distinct from Pro:
  - engaged: sent >=1 outreach OR dashboard_view_count >= base p90
  - intense: sent >=5 outreach  (nested subset of engaged)

Connection-agnostic: callers inject run_query(sql) -> DataFrame (analytics/lib/databricks_conn).
Includes a self-contained Wilson interval (wilson_ci, same formula as
analytics/lib/win_analysis.wilson) so this module runs inside the Databricks workspace
without analytics/lib on the path.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd

CAT = "goodparty_data_catalog"
UWC = f"{CAT}.mart_analytics.users_win_candidacy"
CAND = f"{CAT}.mart_civics.candidacy"
BASE = f"{CAT}.mart_analytics.users_win_base"
MILE = f"{CAT}.dbt.int__amplitude_user_milestones"

WINDOW_START = "2025-01-01"
WINDOW_END = "2026-12-31"
VOTER_FLOOR = 1000

# Pre-registered voter_count bands (brief cohorts.voter_count_band).
VOTER_BANDS = [1000, 5000, 25000, 100000, np.inf]
VOTER_BAND_LABELS = ["1k-5k", "5k-25k", "25k-100k", "100k+"]

# Pre-registered intense-outreach threshold (brief power_user_definition).
INTENSE_CAMPAIGNS = 5

# Viability 2.0 band order (win-analytics-knowledge canonical_metrics / viability.md).
VIABILITY_ORDER = ["No Chance", "Unlikely to Win", "Has a Chance", "Likely to Win", "Frontrunner"]


def build_working_set(run_query: Callable[[str], pd.DataFrame]) -> pd.DataFrame:
    """Return the candidacy-grain working set (one row per gp_candidacy_id).

    Raw pulls only; all derived columns (power-user flags, bands, tenure, outcome,
    election_quarter) are computed in add_derived() so the definitions live in code,
    reviewable, and re-runnable.
    """
    sql = f"""
    SELECT
      d.gp_candidacy_id,
      d.gp_candidate_id,
      c.campaign_id,
      c.user_id,
      c.general_election_date,
      c.voter_count,
      c.l2_district_type,
      c.office_type,
      c.election_level,
      c.campaign_state,
      c.campaign_party,
      c.icp_office_win,
      c.is_pro,
      d.is_incumbent,
      d.latest_stage_reached,
      d.latest_stage_result,
      d.score_viability_automated       AS viability_band_raw,
      d.viability_score                 AS viability_score_num,
      COALESCE(b.is_activated, FALSE)   AS is_activated,
      COALESCE(b.total_campaigns_sent, 0) AS campaigns_sent,
      COALESCE(m.dashboard_view_count, 0) AS dash_views,
      m.pro_upgrade_completed_at
    FROM {UWC} c
    JOIN {CAND} d
      ON c.campaign_id = d.product_campaign_id
     AND c.general_election_date = d.general_election_date
    LEFT JOIN {BASE} b ON b.user_id = c.user_id
    LEFT JOIN {MILE} m ON m.user_id = c.user_id
    WHERE c.is_latest_version AND NOT c.is_demo
      AND c.general_election_date BETWEEN DATE'{WINDOW_START}' AND DATE'{WINDOW_END}'
      AND c.voter_count >= {VOTER_FLOOR}
    """
    return run_query(sql)


def civics_match_coverage(run_query: Callable[[str], pd.DataFrame]) -> pd.DataFrame:
    """Quantify what the working set's inner join to mart_civics.candidacy drops.

    build_working_set() inner-joins users_win_candidacy -> candidacy on
    (campaign_id=product_campaign_id AND general_election_date). Any scoped campaign with no
    civics match is silently excluded. This counts the scoped users_win_candidacy rows and how
    many fail to match, so the exclusion is reported (brief execution_notes step 2) rather than
    hidden. Non-random exclusion (source/recency-correlated) would bias the profile.
    """
    sql = f"""
    WITH scoped AS (
      SELECT c.campaign_version_id, c.campaign_id, c.general_election_date
      FROM {UWC} c
      WHERE c.is_latest_version AND NOT c.is_demo
        AND c.general_election_date BETWEEN DATE'{WINDOW_START}' AND DATE'{WINDOW_END}'
        AND c.voter_count >= {VOTER_FLOOR}
    )
    SELECT
      COUNT(*) AS scoped_rows,
      SUM(CASE WHEN d.product_campaign_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
    FROM scoped s
    LEFT JOIN {CAND} d
      ON s.campaign_id = d.product_campaign_id
     AND s.general_election_date = d.general_election_date
    """
    return run_query(sql)


def add_derived(df: pd.DataFrame, *, dash_p90: float | None = None, asof: pd.Timestamp | None = None):
    """Add derived columns in place-ish (returns a copy). Returns (df, meta) where meta
    records the pinned thresholds so the notebook can pre-register + report them."""
    df = df.copy()
    # UTC to match SQL current_date() (brief boundary_semantics). The local clock could
    # otherwise flip a candidacy whose general_election_date == today's UTC date across the
    # completed/upcoming line.
    asof = asof or pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)

    # Dashboard-engagement threshold: base p90 of dash_views (pinned, pre-registered).
    if dash_p90 is None:
        dash_p90 = float(df["dash_views"].quantile(0.90))

    # Power-user cohorts (usage-defined, nested).
    df["pu_engaged"] = ((df["campaigns_sent"] >= 1) | (df["dash_views"] >= dash_p90)).astype(int)
    df["pu_intense"] = (df["campaigns_sent"] >= INTENSE_CAMPAIGNS).astype(int)

    # Any product touch (the "active" population) vs dormant. The active subset is the
    # acquisition-relevant comparator: engaged/intense are subsets of it by construction, so
    # a within-active over-index strips out the "did anything at all" effect that dominates
    # a full-base comparison (66% of the base is dormant).
    df["any_touch"] = ((df["dash_views"] > 0) | (df["campaigns_sent"] >= 1)).astype(int)

    # Election timing. Three named periods (replaces quarterly cohorting, DATA-2153 rework):
    #   pre-Nov 2025 (odd-year local generals, Jan-Oct 2025), Nov 2025 (the completed cycle),
    #   2026 (mostly upcoming). Used for descriptive trend + portrait-by-period, not as an
    #   acquirable sweep dimension.
    ged = pd.to_datetime(df["general_election_date"])
    df["general_election_date"] = ged
    df["election_period"] = np.select(
        [ged < pd.Timestamp("2025-11-01"), ged < pd.Timestamp("2026-01-01")],
        ["pre-Nov 2025", "Nov 2025"],
        default="2026",
    )
    df["is_completed_general"] = (ged < asof).astype(int)

    # Outcome (won only meaningful for completed generals).
    reached = df["latest_stage_reached"].fillna("")
    result = df["latest_stage_result"].fillna("None")
    is_general_stage = reached.str.startswith("general")
    won = is_general_stage & result.eq("Won")
    lost = is_general_stage & result.eq("Lost")
    df["outcome"] = np.where(
        df["is_completed_general"].eq(0),
        "pending",
        np.where(won, "won", np.where(lost, "lost", "other/unknown")),
    )

    # Constituency-size band (pre-registered edges).
    df["voter_count_band"] = pd.cut(
        df["voter_count"], bins=VOTER_BANDS, labels=VOTER_BAND_LABELS, right=False
    ).astype("object")

    # Viability band (string; ~84% NULL in scope - large 'unknown' bucket by design).
    vb = df["viability_band_raw"].where(df["viability_band_raw"].isin(VIABILITY_ORDER))
    df["viability_band"] = vb.fillna("unknown")

    # Incumbency status. NOTE: this is NOT the 3-way tenure the brief asked for. Prior-run
    # history is absent for ~98% of candidates (only ~1.6% have >1 candidacy on record) and
    # win_number is not a run count, so the first-timer / first re-election / longer-term
    # split is NOT determinable in phase 1 - it is blocked on the deferred web backfill.
    # Named honestly so it is not read as an independent second signal alongside is_incumbent.
    df["incumbency_status"] = (
        df["is_incumbent"].map({True: "incumbent", False: "challenger"}).fillna("unknown")
    )

    # election_level and office_type NULL handling (explicit buckets, never dropped).
    df["election_level"] = df["election_level"].fillna("unknown")
    df["office_type"] = df["office_type"].fillna("unknown")
    df["campaign_party"] = (
        df["campaign_party"]
        .fillna("unknown")
        .str.lower()
        .str.replace(r"\s*party\s*$", "", regex=True)
        .str.strip()
    )
    df["l2_district_type"] = df["l2_district_type"].fillna("unknown")

    # ICP-under-today's-definition (mayor or legislator, constituency > 1000) - OUTPUT ONLY.
    df["is_icp_today"] = df["icp_office_win"].fillna(False).astype(bool)

    meta = {
        "dash_p90": dash_p90,
        "intense_campaigns": INTENSE_CAMPAIGNS,
        "asof": str(asof.date()),
        "n": len(df),
    }
    return df, meta


def wilson_ci(k: int, n: int, z: float = 1.959963984540054) -> tuple[float, float]:
    """Wilson 95% interval on a proportion, returned as (lo, hi) in [0,1]. NaN if n==0."""
    if n == 0:
        return (np.nan, np.nan)
    p = k / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    margin = z * np.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / den
    return (max(0.0, center - margin), min(1.0, center + margin))


def over_index(df: pd.DataFrame, dim: str, cohort_col: str, *, n_floor: int = 30) -> pd.DataFrame:
    """Single-dimension over-index of a power-user cohort vs the base.

    For each value of `dim`: base share of that value, the cohort's rate within that value
    (cohort_col mean), the base cohort rate, over-index = cell_rate / base_rate, and Wilson
    95% CI on the cell rate. n = base rows in the cell.
    """
    base_rate = df[cohort_col].mean()
    rows = []
    for val, g in df.groupby(dim, dropna=False):
        n = len(g)
        k = int(g[cohort_col].sum())
        rate = k / n if n else np.nan
        lo, hi = wilson_ci(k, n)
        rows.append(
            {
                dim: val,
                "n_base": n,
                "base_share_pct": round(100 * n / len(df), 1),
                "cohort_in_cell": k,
                "cohort_rate_pct": round(100 * rate, 1),
                "base_rate_pct": round(100 * base_rate, 1),
                "over_index": round(rate / base_rate, 2) if base_rate else np.nan,
                "ci_lo_pct": round(100 * lo, 1),
                "ci_hi_pct": round(100 * hi, 1),
                "below_floor": n < n_floor,
            }
        )
    out = pd.DataFrame(rows).sort_values("over_index", ascending=False, na_position="last")
    return out.reset_index(drop=True)


def combination_sweep(
    df: pd.DataFrame,
    dims: list[str],
    cohort_col: str,
    *,
    max_order: int = 3,
    n_floor: int = 30,
    top: int = 40,
    direction: str = "over",
) -> tuple[pd.DataFrame, dict]:
    """Deterministic multi-way over-index sweep (brief combination_sweep).

    Enumerate combinations of `dims` of order 1..max_order; group by each; keep only cells
    with base N >= n_floor; compute cohort over-index vs base + Wilson CI; rank by a
    DIRECTIONAL support-weighted lift = (over_index - 1) * sqrt(n). With direction="over"
    (default) the table is sorted so the most over-represented combined segments lead -
    "power users look like this"; direction="under" flips it to surface deficits. Returns
    (top_table, log).

    Exploratory / hypothesis-generating: many cells tested, so treat as candidate segments
    feeding DATA-2154, not confirmatory. The log records combinations and cells evaluated
    vs kept so truncation is visible.
    """
    from itertools import combinations

    base_rate = df[cohort_col].mean()
    if not base_rate:
        raise ValueError(
            f"combination_sweep: base_rate is 0 for cohort_col={cohort_col!r}: all rows are 0 "
            "in the supplied DataFrame, so lift_score would be NaN for every cell."
        )
    results = []
    n_combos = 0
    n_cells_eval = 0
    n_cells_kept = 0
    for order in range(1, max_order + 1):
        for combo in combinations(dims, order):
            n_combos += 1
            grouped = df.groupby(list(combo), dropna=False)
            for key, g in grouped:
                n_cells_eval += 1
                n = len(g)
                if n < n_floor:
                    continue
                n_cells_kept += 1
                k = int(g[cohort_col].sum())
                rate = k / n
                # Keep rate==0 cells: over_index is a valid 0.0 and the strongest deficit
                # (most negative lift_score), which direction="under" must surface, not drop.
                lo, hi = wilson_ci(k, n)
                oi = rate / base_rate if base_rate else np.nan
                key_t = key if isinstance(key, tuple) else (key,)
                results.append(
                    {
                        "order": order,
                        "dims": " x ".join(combo),
                        "segment": " | ".join(f"{d}={v}" for d, v in zip(combo, key_t, strict=False)),
                        "n_base": n,
                        "cohort_in_cell": k,
                        "cohort_rate_pct": round(100 * rate, 1),
                        "base_rate_pct": round(100 * base_rate, 1),
                        "over_index": round(oi, 2),
                        "ci_lo_pct": round(100 * lo, 1),
                        "ci_hi_pct": round(100 * hi, 1),
                        "lift_score": round((oi - 1) * np.sqrt(n), 2),
                    }
                )
    out = pd.DataFrame(results)
    # Directional support-weighted lift: over-represented segments lead (direction="over"),
    # or deficits lead (direction="under").
    if len(out):
        ascending = direction == "under"
        out = out.sort_values("lift_score", ascending=ascending).reset_index(drop=True)
    log = {
        "combinations_evaluated": n_combos,
        "cells_evaluated": n_cells_eval,
        "cells_kept_ge_floor": n_cells_kept,
        "cells_dropped_below_floor": n_cells_eval - n_cells_kept,
        "n_floor": n_floor,
        "max_order": max_order,
    }
    return out.head(top), log
