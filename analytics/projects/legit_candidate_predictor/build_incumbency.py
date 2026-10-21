"""DATA-2239 Area 2 build 2: person-level incumbency at signup.

Implements the argument behind keeping the position-level holder block, but at the grain the
argument actually describes: is *this signup* an office holder, not how many holders the seat has.
`ballotready_s3_office_holders_v3` is person-level (first/middle/last name, state, term dates,
position), so this needs no new source and no new ticket, just the right grain.

Why not use the existing flag: `mart_civics.candidacy.is_incumbent` barely discriminates on this
population (70.2% corroboration when true against 68.3% when false, with its null bucket at 33.6%
acting as yet another coverage indicator). That is the pre-rebuild, TechSpeed-derived version; the
BR-sourced rebuild is a separate open ticket. This build is the predictor-side feature and should be
replaced by the governed flag once that lands.

PIT rule, and it is the whole ballgame here: only terms with `start_at <= signup_date` count. A
term that begins *after* signup is frequently the term won in the very election being predicted, so
counting it would be direct outcome leakage. The gate is on signup date rather than election date
because signup is the anchor the pass-B contract uses.

Precision: matching is on (state, upper first name, upper last name) against 366,607 distinct
name-state keys, so common names will collide. `inc_name_ambiguity` carries the collision count so
the model can discount rather than being silently misled.

Outputs `datasets/incumbency_features.parquet` at user grain, plus a profile and an intake review.

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_incumbency.py
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

SQL = """
with pop as (
  select u.user_id,
         cast(u.user_created_at as date)          as signup_date,
         u.campaign_state,
         upper(trim(u.user_first_name))           as fn,
         upper(trim(u.user_last_name))            as ln,
         cast(u.ballotready_position_id as bigint) as br_position_id
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  where u.is_latest_version and not u.is_demo
    and u.user_first_name is not null and u.user_last_name is not null
    and u.campaign_state is not null
  group by 1, 2, 3, 4, 5, 6
),
oh as (
  select upper(trim(first_name))          as fn,
         upper(trim(last_name))           as ln,
         state,
         cast(br_position_id as bigint)   as br_position_id,
         br_normalized_position_id,
         cast(start_at as date)           as start_at,
         cast(end_at as date)             as end_at,
         br_office_holder_id,
         is_vacant
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_office_holders_v3
  where first_name is not null and last_name is not null and state is not null
),
-- PIT gate: the term must already have begun at signup. A term starting later is often the one
-- won in the election we are predicting.
matched as (
  select p.user_id,
         p.signup_date,
         p.br_position_id                                   as running_for_position,
         count(*)                                           as n_terms,
         count(distinct oh.br_office_holder_id)              as n_offices,
         count(distinct oh.br_position_id)                   as n_positions_held,
         min(oh.start_at)                                    as earliest_term_start,
         max(oh.start_at)                                    as latest_term_start,
         max(oh.end_at)                                      as latest_term_end,
         max(case when oh.br_position_id = p.br_position_id then 1 else 0 end) as holds_same_position,
         max(case when oh.end_at >= p.signup_date then 1 else 0 end)           as serving_at_signup,
         max(case when oh.is_vacant then 1 else 0 end)                          as any_vacant_record
  from pop p
  join oh
    on oh.fn = p.fn and oh.ln = p.ln and oh.state = p.campaign_state
   and oh.start_at <= p.signup_date
  group by p.user_id, p.signup_date, p.br_position_id
),
-- How many distinct people share this name in this state, gate-free. A high count means the match
-- above is unreliable, which is information rather than a reason to drop the row.
ambiguity as (
  select p.user_id, count(distinct oh.br_office_holder_id) as name_state_holder_records
  from pop p
  join oh on oh.fn = p.fn and oh.ln = p.ln and oh.state = p.campaign_state
  group by p.user_id
)
select p.user_id,
       m.n_terms, m.n_offices, m.n_positions_held,
       m.earliest_term_start, m.latest_term_start, m.latest_term_end,
       m.holds_same_position, m.serving_at_signup, m.any_vacant_record,
       a.name_state_holder_records,
       p.signup_date
from pop p
left join matched m on m.user_id = p.user_id
left join ambiguity a on a.user_id = p.user_id
"""


def to_user_grain(raw: pd.DataFrame) -> pd.DataFrame:
    """Collapse to one row per user.

    A user with two campaigns appears once per declared position, since `holds_same_position` is
    position-specific. Take the most favourable reading per user: holding any of the seats they ran
    for counts, and term windows take their outer bounds.
    """
    agg = {
        "signup_date": "first",
        "n_terms": "max",
        "n_offices": "max",
        "n_positions_held": "max",
        "earliest_term_start": "min",
        "latest_term_start": "max",
        "latest_term_end": "max",
        "holds_same_position": "max",
        "serving_at_signup": "max",
        "any_vacant_record": "max",
        "name_state_holder_records": "max",
    }
    return raw.groupby("user_id", as_index=False).agg(agg)


def build(raw: pd.DataFrame) -> pd.DataFrame:
    signup = pd.to_datetime(raw["signup_date"])
    out = pd.DataFrame({"user_id": raw["user_id"]})

    matched = raw["n_terms"].notna()
    out["inc_is_officeholder_at_signup"] = matched.astype(int)
    out["inc_serving_at_signup"] = np.where(matched, raw["serving_at_signup"], 0)
    out["inc_holds_same_position"] = np.where(matched, raw["holds_same_position"], 0)

    # Zeros here are GENUINE, not sentinels (settled 2026-08-06): the join is exhaustive over BR's
    # office-holder table, so no match means no holder record with this name in this state. That is
    # a real "not an office holder" rather than "unknown", which is the opposite of the
    # n_br_candidacies_by_deadline case where the absence was a coverage gap.
    out["inc_n_offices"] = raw["n_offices"].fillna(0)
    # inc_n_positions_held dropped: 0.990 correlated with inc_n_offices (intake review).

    years = (signup - pd.to_datetime(raw["earliest_term_start"])).dt.days / 365.25
    out["inc_years_in_office_at_signup"] = np.where(matched, years, np.nan)
    to_end = (pd.to_datetime(raw["latest_term_end"]) - signup).dt.days
    out["inc_days_to_term_end"] = np.where(matched, to_end, np.nan)
    out["inc_term_ends_within_1y"] = np.where(
        matched & raw["latest_term_end"].notna(), (to_end.between(0, 365)).astype(float), np.nan
    )

    # Precision guard: how many holder records share this name and state at all.
    out["inc_name_ambiguity"] = raw["name_state_holder_records"].fillna(0)
    out["inc_match_is_unique"] = np.where(
        raw["name_state_holder_records"].notna(), raw["name_state_holder_records"].eq(1).astype(float), 0.0
    )
    return out


if __name__ == "__main__":
    raw = run_query(SQL)
    print(
        f"rows: {len(raw):,} (user x declared position)  "
        f"matched to a pre-signup term: {raw.n_terms.notna().mean():.1%}"
    )
    raw = to_user_grain(raw)
    print(f"users: {len(raw):,}")
    assert raw["user_id"].is_unique, "one row per user expected"

    feats = build(raw)
    print(f"features: {feats.shape[1] - 1}")
    print(f"  officeholder at signup:  {feats.inc_is_officeholder_at_signup.mean():.1%}")
    print(f"  serving at signup:       {feats.inc_serving_at_signup.mean():.1%}")
    print(f"  holds the same position:  {feats.inc_holds_same_position.mean():.1%}")

    out = HERE / "datasets" / "incumbency_features.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}")

    write_profile(
        feats,
        "incumbency_features",
        note="Person-level office-holder match, PIT-gated to terms beginning at or before "
        "signup. Name-based match, so inc_name_ambiguity carries collision risk.",
    )
    write_intake(
        feats,
        "incumbency_features",
        key="user_id",
        source="dbt.stg_airbyte_source__ballotready_s3_office_holders_v3 (person grain)",
        join="(state, first name, last name) -> office holders, gated start_at <= signup",
        pit_class="B (term must begin at or before signup; later terms excluded as outcome " "leakage)",
    )
