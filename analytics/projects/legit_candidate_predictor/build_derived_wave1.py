"""DATA-2239 stage 7, wave 1: derived variables HGB cannot learn on its own.

Four variables from DERIVED_VARIABLES.md (seed list, 2026-08-12), all label-free:

True ratios (class 1) — the quotient is the concept, the raw columns are not:
- 1.1 drv_signature_burden = pos_signature_count / voter_count. The intake cross-family check
  flagged the raw count at r=0.968 with voter_count, i.e. the count IS district size; the ratio
  is the residual (petition burden relative to the electorate).
- 1.2 drv_fee_per_voter = pos_max_filing_fee / voter_count. Raw fee ranked near-bottom in the
  BR family; cost of entry is a relative concept ($500 filters a village, not a city).

Cohort-relative (class 3) — row vs peer-group statistic, needs cross-row aggregation:
- 3.2 drv_voter_count_vs_office_norm = log(voter_count / trailing median for office_type).
  "Big race" separated from "big office type".
- 3.4 drv_days_to_election_vs_office_norm = days_signup_to_election - trailing median for
  office_type. Lead time is the label's biggest structural driver (era-drift finding); the
  office-norm residual may separate behavior from calendar mechanics.

PIT: trailing statistics use STRICTLY EARLIER signups only (expanding median, self excluded,
minimum 30 prior observations else NaN), so the value is computable at scoring time and no
future signup leaks backward. Ratios inherit their ingredients' availability (a-priori race
properties + district size).

Expected intake flags, pre-answered: the ratios will correlate with their ingredients — that is
the point, the experiment decides whether the residual carries anything; office_type NaN groups
get NaN norms (no fallback to a global median, which would smuggle in a coverage indicator).

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_derived_wave1.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from profile import write_profile  # noqa: E402

from databricks_conn import run_query  # noqa: E402
from intake import write_intake  # noqa: E402

MIN_PRIOR = 30  # observations required before a trailing median is trusted

SQL = """
select u.user_id,
       cast(u.user_created_at as date)            as signup_date,
       upper(trim(u.office_type))                 as office_type,
       cast(u.ballotready_position_id as bigint)  as br_position_id,
       u.voter_count,
       datediff(u.election_date, cast(u.user_created_at as date)) as days_signup_to_election
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
where u.is_latest_version and not u.is_demo
qualify row_number() over (partition by u.user_id
                           order by u.election_date desc nulls last, u.campaign_id desc) = 1
"""


def trailing_median(df: pd.DataFrame, value_col: str, group_col: str) -> pd.Series:
    """Expanding median of value_col over STRICTLY EARLIER signups within group_col.

    Sorted by signup date; ties (same-day signups) are excluded from each other's window via
    a per-date block shift, so no same-day peer influences the statistic. NaN until MIN_PRIOR
    prior observations exist.
    """
    out = pd.Series(np.nan, index=df.index)
    for _, g in df.groupby(group_col, dropna=True):
        g = g.sort_values("signup_date")
        vals, dates = g[value_col].to_numpy(dtype=float), g["signup_date"].to_numpy()
        med = np.full(len(g), np.nan)
        history: list[float] = []
        i = 0
        while i < len(g):
            j = i
            while j < len(g) and dates[j] == dates[i]:
                j += 1
            if len(history) >= MIN_PRIOR:
                m = float(np.median(history))
                med[i:j] = m
            history.extend(v for v in vals[i:j] if not np.isnan(v))
            i = j
        out.loc[g.index] = med
    return out


def build(raw: pd.DataFrame, br: pd.DataFrame) -> pd.DataFrame:
    df = raw.merge(
        br[["br_position_id", "pos_signature_count", "pos_max_filing_fee"]], on="br_position_id", how="left"
    )
    df["signup_date"] = pd.to_datetime(df["signup_date"])
    vc = pd.to_numeric(df["voter_count"], errors="coerce")
    vc = vc.where(vc > 0)

    out = pd.DataFrame({"user_id": df["user_id"]})
    out["drv_signature_burden"] = pd.to_numeric(df["pos_signature_count"], errors="coerce") / vc
    out["drv_fee_per_voter"] = pd.to_numeric(df["pos_max_filing_fee"], errors="coerce") / vc

    df["_vc"] = vc
    norm = trailing_median(df, "_vc", "office_type")
    out["drv_voter_count_vs_office_norm"] = np.log(vc / norm)

    dse = pd.to_numeric(df["days_signup_to_election"], errors="coerce")
    df["_dse"] = dse
    dnorm = trailing_median(df, "_dse", "office_type")
    out["drv_days_to_election_vs_office_norm"] = dse - dnorm
    return out


if __name__ == "__main__":
    raw = run_query(SQL)
    br = pd.read_parquet(HERE / "datasets" / "br_position_features.parquet")
    print(f"users: {len(raw):,}")
    assert raw["user_id"].is_unique

    feats = build(raw, br)
    for c in feats.columns[1:]:
        print(f"  {c}: fill {feats[c].notna().mean():.1%}  median {feats[c].median():.4g}")

    out = HERE / "datasets" / "derived_wave1_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}")

    write_profile(
        feats,
        "derived_wave1_features",
        note="Stage-7 wave 1: two entry-cost ratios + two trailing office-norm "
        "residuals. Label-free; trailing medians use strictly earlier signups "
        "(min 30) so PIT holds at scoring time.",
    )
    write_intake(
        feats,
        "derived_wave1_features",
        key="user_id",
        source="users_win_candidacy + br_position_features.parquet",
        join="user_id grain; ratios via br_position_id, norms via (office_type, " "trailing signup window)",
        pit_class="B (a-priori race properties; cohort stats from prior signups only)",
    )
