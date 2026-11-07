"""DATA-2239 Area 2 build 4: L2 person match (easy-match pass, pre-DATA-2244).

Scope decided 2026-08-12 (Tristan): exact (declared state, first name, last name) matches,
resolving to a single voter record when the match is unique OR when exactly one of a 2-5 match
set shares the user's zip. Everything else (nicknames, edit distance, nationwide, middle-name
tiebreaks) is matcha's job under DATA-2244; unresolved users stay NaN + indicator so the matcha
bridge can replace this one without a schema change.

Two artifacts:
- datasets/l2_match_bridge.parquet — user_id -> LALVOTERID + match_type + ambiguity counts.
  This is the mart-asset prototype (engineered connections are mart assets, 2026-08-07) and the
  only file carrying the voter id.
- datasets/l2_features.parquet — user-grain model features, no identifiers.

Leakage discipline (established on incumbency, re-confirmed on gp_user_details): the match is
name-based and the corroboration label is name-matched ER, so ambiguity counts are the
name-commonness leak channel. n_name_matches / n_zip_matches / match_type live in the BRIDGE as
diagnostics and never enter the feature file; even l2_matched partly encodes name-matchability
and gets read with that caveat.

PIT: the L2 snapshot is current, not as-of-signup (same acceptance as the BR position family).
Hard gates where the leak is outcome-shaped rather than drift-shaped:
- vote history counts use only election years STRICTLY BEFORE the signup year
  (AnyElection_<signup year>+ is contemporaneous with running);
- years_registered is NaN when the registration date is after signup (registering after signup
  is often registering TO run);
- FEC donation count/amount are used only when the last donation predates signup, because the
  cumulative totals otherwise include post-signup giving.

Haystaq shortlist (11 columns, pre-declared in SOURCE_CATALOG 4.5 — do not widen without a
decision): modeled propensities keyed on LALVOTERID, attached only for resolved matches.

Reserve-tier candidates (two-model design, marked at intake): the registration/vote-history
block — "are high scorers registered voters" is a claim someone will want.

Deliberately NOT built in v0: SES block (equity-sensitive, needs its own review), lat/lng
district distance (needs a centroid source), phone crosscheck, nationwide match.

Run (slow join against 218M rows; run alone):
  uv run --with pyarrow python projects/legit_candidate_predictor/build_l2_match.py
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

GENERAL_YEARS = list(range(2012, 2024, 2))  # General_2012 .. General_2022
ANY_YEARS = list(range(2011, 2026, 2))  # AnyElection_2011 .. AnyElection_2025
VOTE_COLS = [f"General_{y}" for y in GENERAL_YEARS] + [f"AnyElection_{y}" for y in ANY_YEARS]

HAYSTAQ = [
    "hs_activism",
    "hs_political_donations_likely",
    "hs_partisanship_moderate_third_party_support",
    "hs_ideology_overall_party_indep",
    "hs_campaign_finance_reform_high_priority",
    "hs_rank_choice_voting_support",
    "hs_likely_presidential_voter",
    "hs_likely_mid_term_voter",
    "hs_likely_polling_turnout",
    "hs_responsiveness_email",
    "hs_responsiveness_sms",
]

_L2_ATTRS = """
       l.LALVOTERID, l.Voters_Active, l.Parties_Description, l.Voters_BirthDate, l.Voters_Age,
       l.Voters_CalculatedRegDate, l.Voters_VotingPerformanceEvenYearGeneral,
       l.Residence_Families_HHVotersCount, l.Voters_MovedFrom_Date,
       l.FECDonors_NumberOfDonations, l.FECDonors_TotalDonationsAmount,
       l.FECDonors_LastDonationDate,
""" + ",\n".join(f"       l.{c}" for c in VOTE_COLS)

SQL = f"""
with pop as (
  select u.user_id,
         cast(u.user_created_at as date)           as signup_date,
         u.campaign_state,
         upper(trim(u.user_first_name))            as fn,
         upper(trim(u.user_last_name))             as ln,
         substring(trim(u.user_zip), 1, 5)         as zip5,
         upper(trim(u.campaign_party))             as campaign_party
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  where u.is_latest_version and not u.is_demo
    and u.user_first_name is not null and u.user_last_name is not null
    and u.campaign_state is not null
  -- one row per user: a user with several campaigns would corrupt the per-user match counts
  -- (window partitions by user_id) and duplicate the final grain
  qualify row_number() over (partition by u.user_id
                             order by u.election_date desc nulls last, u.campaign_id desc) = 1
),
matches as (
  select p.user_id, p.signup_date, p.zip5, p.campaign_party,
{_L2_ATTRS},
         substring(l.Residence_Addresses_Zip, 1, 5)                          as l2_zip5,
         count(*)              over (partition by p.user_id)                 as n_matches,
         sum(case when substring(l.Residence_Addresses_Zip, 1, 5) = p.zip5
                  then 1 else 0 end)
                               over (partition by p.user_id)                 as n_zip_matches,
         case when substring(l.Residence_Addresses_Zip, 1, 5) = p.zip5
              then 1 else 0 end                                              as zip_ok
  from pop p
  join goodparty_data_catalog.dbt.int__l2_nationwide_uniform l
    on l.Residence_Addresses_State = p.campaign_state
   and upper(trim(l.Voters_FirstName)) = p.fn
   and upper(trim(l.Voters_LastName))  = p.ln
),
resolved as (
  select *,
         case when n_matches = 1 then 'unique' else 'zip_resolved' end as match_type
  from matches
  where n_matches = 1
     or (n_matches between 2 and 5 and n_zip_matches = 1 and zip_ok = 1)
),
counts as (
  select user_id, min(n_matches) as n_name_matches, min(n_zip_matches) as n_zip_matches
  from matches group by user_id
)
select p.user_id, p.signup_date, p.campaign_party,
       c.n_name_matches, c.n_zip_matches,
       r.match_type, r.LALVOTERID,
       r.Voters_Active, r.Parties_Description, r.Voters_BirthDate, r.Voters_Age,
       r.Voters_CalculatedRegDate, r.Voters_VotingPerformanceEvenYearGeneral,
       r.Residence_Families_HHVotersCount, r.Voters_MovedFrom_Date,
       r.FECDonors_NumberOfDonations, r.FECDonors_TotalDonationsAmount,
       r.FECDonors_LastDonationDate,
{",".join(f"       r.{c}" for c in VOTE_COLS)},
{",".join(f"       h.{c}" for c in HAYSTAQ)}
from pop p
left join counts c on c.user_id = p.user_id
left join resolved r on r.user_id = p.user_id
left join goodparty_data_catalog.dbt.int__l2_nationwide_haystaq_scores h
  on h.LALVOTERID = r.LALVOTERID
"""


# A-priori party normalisation (product free text vs L2 Parties_Description). Written from the
# two systems' documented vocabularies, not tuned on labels.
def _party_norm(s: pd.Series) -> pd.Series:
    x = s.fillna("").astype(str).str.upper().str.strip()
    out = pd.Series("other", index=s.index)
    out[x.str.contains("DEMOCRAT")] = "democratic"
    out[x.str.contains("REPUBLICAN")] = "republican"
    out[x.str.contains("LIBERTARIAN")] = "libertarian"
    out[x.str.contains("GREEN")] = "green"
    out[x.str.contains("INDEPENDEN|NON.?PARTISAN|NO PARTY|UNAFFILIATED|DECLINE")] = "independent"
    out[x.eq("")] = ""
    return out


def _voted(s: pd.Series) -> pd.Series:
    return (s.notna() & s.astype(str).str.strip().ne("")).astype(int)


def build(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    signup = pd.to_datetime(raw["signup_date"])
    signup_year = signup.dt.year
    resolved = raw["match_type"].notna()

    bridge = raw.loc[resolved, ["user_id", "LALVOTERID", "match_type"]].copy()
    bridge = bridge.merge(raw[["user_id", "n_name_matches", "n_zip_matches"]], on="user_id", how="left")

    out = pd.DataFrame({"user_id": raw["user_id"]})
    out["l2_matched"] = resolved.astype(int)

    active = raw["Voters_Active"].astype(str).str.upper().str.strip()
    out["l2_is_active"] = np.where(resolved, active.isin(["A", "ACTIVE"]).astype(float), np.nan)

    reg = pd.to_datetime(raw["Voters_CalculatedRegDate"], errors="coerce")
    yrs = (signup - reg).dt.days / 365.25
    # Registration after signup is post-signup information (often registering TO run): NaN.
    out["l2_years_registered_at_signup"] = np.where(resolved & (yrs >= 0), yrs, np.nan)

    birth = pd.to_datetime(raw["Voters_BirthDate"], errors="coerce")
    age = (signup - birth).dt.days / 365.25
    out["l2_age_at_signup"] = np.where(resolved & age.between(17, 105), age, np.nan)

    perf = pd.to_numeric(
        raw["Voters_VotingPerformanceEvenYearGeneral"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
    )
    out["l2_vote_propensity_general"] = np.where(resolved, perf, np.nan)

    gen = pd.DataFrame({y: _voted(raw[f"General_{y}"]) for y in GENERAL_YEARS})
    anye = pd.DataFrame({y: _voted(raw[f"AnyElection_{y}"]) for y in ANY_YEARS})
    prior_gen = gen.apply(lambda col: col.where(col.name < signup_year, 0)).sum(axis=1)
    prior_any = anye.apply(lambda col: col.where(col.name < signup_year, 0)).sum(axis=1)
    out["l2_n_generals_voted_pre_signup"] = np.where(resolved, prior_gen, np.nan)
    out["l2_n_oddyear_votes_pre_signup"] = np.where(resolved, prior_any, np.nan)

    last_vote = pd.concat([gen, anye], axis=1).apply(
        lambda row: max((y for y, v in row.items() if v and y < signup_year.loc[row.name]), default=np.nan),
        axis=1,
    )
    out["l2_years_since_last_vote_at_signup"] = np.where(resolved, signup_year - last_vote, np.nan)

    out["l2_party_matches_declared"] = np.where(
        resolved & raw["campaign_party"].notna() & raw["Parties_Description"].notna(),
        (_party_norm(raw["campaign_party"]) == _party_norm(raw["Parties_Description"])).astype(float),
        np.nan,
    )

    out["l2_hh_voters"] = np.where(
        resolved, pd.to_numeric(raw["Residence_Families_HHVotersCount"], errors="coerce"), np.nan
    )

    moved = pd.to_datetime(raw["Voters_MovedFrom_Date"], errors="coerce")
    days_since_move = (signup - moved).dt.days
    out["l2_moved_within_2y_pre_signup"] = np.where(
        resolved & moved.notna(), days_since_move.between(0, 730).astype(float), np.nan
    )

    last_don = pd.to_datetime(raw["FECDonors_LastDonationDate"], errors="coerce")
    don_gate = resolved & last_don.notna() & (last_don <= signup)
    out["l2_fec_n_donations_pre_signup"] = np.where(
        don_gate, pd.to_numeric(raw["FECDonors_NumberOfDonations"], errors="coerce"), np.nan
    )
    out["l2_fec_total_donated_pre_signup"] = np.where(
        don_gate, pd.to_numeric(raw["FECDonors_TotalDonationsAmount"], errors="coerce"), np.nan
    )
    # Never-donor vs post-signup-donor vs unknown is knowable; a real 0 only for resolved
    # matches with no FEC record at all.
    out["l2_fec_donor_pre_signup"] = np.where(
        resolved, np.where(don_gate, 1.0, np.where(last_don.isna(), 0.0, np.nan)), np.nan
    )

    for c in HAYSTAQ:
        out[f"l2_{c}"] = np.where(resolved, pd.to_numeric(raw[c], errors="coerce"), np.nan)

    return out, bridge


if __name__ == "__main__":
    raw = run_query(SQL)
    print(f"users: {len(raw):,}")
    assert raw["user_id"].is_unique, "resolved join must stay one row per user"

    feats, bridge = build(raw)
    n = len(feats)
    print(
        f"matched (resolved): {feats.l2_matched.mean():.1%}  "
        f"(unique {raw.match_type.eq('unique').sum():,}, "
        f"zip_resolved {raw.match_type.eq('zip_resolved').sum():,})"
    )
    print(f"any name match: {raw.n_name_matches.notna().mean():.1%}")
    print(f"features: {feats.shape[1] - 1}")

    bpath = HERE / "datasets" / "l2_match_bridge.parquet"
    bridge.to_parquet(bpath, index=False)
    fpath = HERE / "datasets" / "l2_features.parquet"
    feats.to_parquet(fpath, index=False)
    print(f"saved -> {fpath}  +  {bpath} (bridge, carries LALVOTERID)")

    write_profile(
        feats,
        "l2_features",
        note="L2 person match, easy-match pass (exact-unique + zip-resolved). "
        "Attributes only for resolved matches; ambiguity counts live in the "
        "bridge as diagnostics (name-commonness leak channel). Current-snapshot "
        "PIT caveat; vote history / registration / FEC hard-gated to pre-signup.",
    )
    write_intake(
        feats,
        "l2_features",
        key="user_id",
        source="dbt.int__l2_nationwide_uniform + int__l2_nationwide_haystaq_scores",
        join="(campaign_state, first, last) exact; resolve unique or zip-unique of 2-5; "
        "haystaq via resolved LALVOTERID",
        pit_class="B with current-snapshot caveat (as BR position); vote history "
        "strictly pre-signup-year; reg-after-signup and post-signup FEC NaN'd. "
        "RESERVE candidates: registration/vote-history block",
    )
