"""DATA-2239 stage 7, wave 2: race-context priors from the PRIOR election cycle (3.5).

The one deliberately label-using derived variable: historical corroboration rate by race
context, from `working_set_2024.parquet` (8,906 users, 2024 general elections, base 45.0%).

Why NO fold-internal machinery is needed (this kills the planned exp_family extension):
every working-set row has election year >= 2025, and the priors use ONLY 2024-election labels,
which are outside the working set entirely. The label data is therefore disjoint from every CV
fold and strictly earlier in time than every row it scores — time-legal and fold-legal as a
STATIC column. The fold-internal harness extension becomes necessary only if we later add
within-working-set priors (2025-election labels applied to 2026-election rows); that variant
is deferred and must NOT be built without it.

PIT gate on the applied side: rows whose own election year is <= 2024 (possible in the full
user base, impossible in the working set) get NaN — a 2024-cycle rate is not "prior" for them.

Groupings (min {MIN_N} 2024 observations, else NaN; no shrinkage in v0 — a hard support gate
is easier to reason about than empirical-Bayes, revisit only if the family earns its keep):
- br_normalized_position_type (23 groups at n>=30 in 2024)
- campaign_state (46)
- (campaign_state, office_type) (sparser; the min-n gate does the work)

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_race_priors.py
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

MIN_N = 30

SQL = """
select u.user_id,
       upper(trim(u.br_normalized_position_type)) as position_type,
       upper(trim(u.campaign_state))              as campaign_state,
       upper(trim(u.office_type))                 as office_type,
       year(u.election_date)                      as election_year
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
where u.is_latest_version and not u.is_demo
qualify row_number() over (partition by u.user_id
                           order by u.election_date desc nulls last, u.campaign_id desc) = 1
"""


def rates(prior: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    g = (
        prior.dropna(subset=keys)
        .groupby(keys, observed=True)["corroborated"]
        .agg(rate="mean", n="size")
        .reset_index()
    )
    return g[g["n"] >= MIN_N].drop(columns="n")


def build(raw: pd.DataFrame, prior: pd.DataFrame) -> pd.DataFrame:
    prior = prior.copy()
    for c, src in [
        ("position_type", "br_normalized_position_type"),
        ("campaign_state", "campaign_state"),
        ("office_type", "office_type"),
    ]:
        prior[c] = prior[src].astype(str).str.upper().str.strip().replace("NAN", np.nan)

    out = pd.DataFrame({"user_id": raw["user_id"]})
    specs = {
        "drv_prior24_rate_position_type": ["position_type"],
        "drv_prior24_rate_state": ["campaign_state"],
        "drv_prior24_rate_state_office": ["campaign_state", "office_type"],
    }
    for name, keys in specs.items():
        r = rates(prior, keys).rename(columns={"rate": name})
        out[name] = raw.merge(r, on=keys, how="left")[name].to_numpy()

    # A 2024-cycle rate is only PRIOR information for elections after 2024.
    not_prior = raw["election_year"].notna() & (raw["election_year"] <= 2024)
    out.loc[not_prior.to_numpy(), list(specs)] = np.nan
    return out


if __name__ == "__main__":
    raw = run_query(SQL)
    prior = pd.read_parquet(HERE / "datasets" / "working_set_2024.parquet")
    print(f"users: {len(raw):,}  prior cycle: {len(prior):,} rows, base {prior.corroborated.mean():.3f}")
    assert raw["user_id"].is_unique

    feats = build(raw, prior)
    for c in feats.columns[1:]:
        print(
            f"  {c}: fill {feats[c].notna().mean():.1%}  spread "
            f"[{feats[c].min():.3f}, {feats[c].max():.3f}]"
        )

    out = HERE / "datasets" / "race_prior_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}")

    write_profile(
        feats,
        "race_prior_features",
        note="2024-cycle corroboration rates by race context, min 30 support. Static "
        "and fold-legal because the 2024 labels are disjoint from the working "
        "set; rows with election year <= 2024 are NaN'd (not prior for them).",
    )
    write_intake(
        feats,
        "race_prior_features",
        key="user_id",
        source="working_set_2024.parquet (2024-cycle labels) applied to " "users_win_candidacy race context",
        join="(position_type / state / state x office_type) group rates -> user grain",
        pit_class="B for all working-set rows (2024 labels strictly prior + fold-"
        "disjoint); within-ws 2025->2026 priors DEFERRED pending fold-internal "
        "harness support",
    )
