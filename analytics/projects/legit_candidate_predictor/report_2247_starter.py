"""Starter cuts for the DATA-2247 sales/product report: what do legitimate 2026 users look like?

Reads `model_predictions.win_candidate_scores_latest` and produces the profile the report is
built on. Every cut here uses the PROFILE score, because every variable being profiled is an
input to the serving model and profiling the serving score on them would restate the scoring
rule rather than discover anything.

Deliberately NOT a finished report. It is the set of cuts that are defensible, run against the
live population, so the analysis can start from numbers rather than from a table schema.

Run: cd analytics && uv run --with pyarrow --with databricks-sql-connector python \
       projects/legit_candidate_predictor/report_2247_starter.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import run_query  # noqa: E402

CAT = "goodparty_data_catalog"
OUT = HERE / "datasets" / "report_2247_cuts.csv"

# The scored view joined to the campaign facts the report profiles on. is_pro and
# icp_office_win are SLICE dimensions in this project, never model inputs, so they are always
# legitimate to profile — on either score.
SQL = f"""
select v.campaign_id, v.user_id,
       v.profile_bucket, v.profile_expected_rate,
       v.serving_bucket, v.serving_expected_rate,
       v.lead_time_days, v.days_to_election, v.deadline_passed,
       u.campaign_state, u.office_type, u.br_normalized_position_type,
       u.campaign_party, u.voter_count, u.is_pro, u.icp_office_win
from {CAT}.model_predictions.win_candidate_scores_latest v
join {CAT}.mart_analytics.users_win_candidacy u
  on u.campaign_id = v.campaign_id and u.is_latest_version and not u.is_demo
"""

ORDER = ["p_under_5", "p_5_15", "p_15_35", "p_35_60", "p_60_plus"]


def share_by(df: pd.DataFrame, col: str, top: int | None = None) -> pd.DataFrame:
    """Composition of each bucket. Read DOWN a column: what this group is made of."""
    t = pd.crosstab(df[col], df["profile_bucket"], normalize="columns").reindex(columns=ORDER).round(3)
    t["_n"] = df[col].value_counts()
    t = t.sort_values("_n", ascending=False)
    return (t.head(top) if top else t).drop(columns="_n")


def rate_by(df: pd.DataFrame, col: str, top: int | None = None) -> pd.DataFrame:
    """Share of each group landing in the top bucket. Read as a lift table."""
    g = (
        df.groupby(col, observed=True)
        .agg(
            n=("campaign_id", "size"),
            pct_top=("profile_bucket", lambda s: (s == "p_60_plus").mean()),
            pct_bottom=("profile_bucket", lambda s: (s == "p_under_5").mean()),
            pro_rate=("is_pro", "mean"),
        )
        .round(3)
    )
    g = g[g["n"] >= 50].sort_values("pct_top", ascending=False)
    return g.head(top) if top else g


def main() -> None:
    df = run_query(SQL)
    df["profile_bucket"] = pd.Categorical(df["profile_bucket"], ORDER, ordered=True)
    print(f"rows {len(df):,}\n")

    print("=== 1. HEADLINE: bucket sizes and what independently corroborates them ===")
    h = (
        df.groupby("profile_bucket", observed=True)
        .agg(
            n=("campaign_id", "size"),
            expected=("profile_expected_rate", "mean"),
            pro_rate=("is_pro", "mean"),
            icp_rate=("icp_office_win", "mean"),
        )
        .round(3)
    )
    h["share"] = (h["n"] / len(df)).round(3)
    print(h.to_string())
    print("\nis_pro and icp_office_win are NOT model inputs, so they are independent checks.")

    print("\n=== 2. OFFICE TYPE: composition of each bucket ===")
    print(share_by(df, "office_type").to_string())
    print("\n=== 2b. OFFICE TYPE: which offices land in the top bucket ===")
    print(rate_by(df, "office_type").to_string())

    print("\n=== 3. STATE: top and bottom 10 by share reaching the top bucket ===")
    st = rate_by(df, "campaign_state")
    print(pd.concat([st.head(10), st.tail(10)]).to_string())

    print("\n=== 4. DISTRICT SIZE ===")
    df["voter_count_band"] = pd.qcut(
        df["voter_count"], 5, duplicates="drop", labels=["smallest", "small", "mid", "large", "largest"]
    )
    print(rate_by(df, "voter_count_band").to_string())

    print("\n=== 5. PARTY — NOT A VALID CUT AS THINGS STAND. Two reasons. ===")
    print("(a) `campaign_party` is an INPUT to the profile model. Only state, office,")
    print("    incumbency and district size were masked, so profiling on party restates")
    print("    the scoring rule. Add 'campaign_party' to CLAIM_MASKS['sales_profile'] and")
    print("    re-run if the report needs this cut.")
    print("(b) The column carries two naming conventions at once — 'Independent' (n=892,")
    print("    27.9% top bucket) alongside 'independent' (n=3,626, 3.2%), and the same for")
    print("    libertarian/green/forward. Both spellings span 2021-2026, so it is not an era")
    print("    split; it looks like which signup flow collected the field, i.e. the")
    print("    entry-channel latent this project already dropped from the model twice.")
    print("    Normalise casing AND establish which flow writes which spelling before")
    print("    quoting any party number.")
    print(rate_by(df, "campaign_party", top=8).to_string())

    print(
        "\n=== 6. TIMING: signup lead time (a serving-model input; profile score is blind "
        "to office/state/size but NOT to lead time — read this one with care) ==="
    )
    df["lead_band"] = pd.cut(
        df["lead_time_days"], [-1, 90, 180, 365, 10**6], labels=["0-90d", "91-180d", "181-365d", "365d+"]
    )
    print(rate_by(df, "lead_band").to_string())

    df.to_csv(OUT, index=False)
    print(f"\nfull joined frame -> {OUT}  ({len(df):,} rows)")


if __name__ == "__main__":
    main()
