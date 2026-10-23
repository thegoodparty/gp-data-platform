"""DATA-2225 MVP step 2: build the working set once, cache to parquet.

Population: users_win_candidacy (latest, not demo), office-declared (version-aware
candidacy join), completed generals 2025-01-01..today. One row per campaign_version.

Column policy (brief step 3b): allowlist only. Candidacy-side tables contribute the
label, the anchors, and race-identity descriptors. No results, stages, runoff,
uncontested, opponent, vote, or viability fields. is_pro / icp_office_win are pulled
as SLICE dimensions, never features.

DATA-2312 rebuild: `n_br_candidacies_by_deadline` is OPPONENTS-ONLY. The row's own BR
candidacy is subtracted when it landed by the deadline, because `br_candidacy_id` is
one of the three ingredients of the label, so counting yourself leaks the label into a
feature. `filing_window_start` is carried for the derived lateness variables.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from databricks_conn import run_query

HERE = Path(__file__).resolve().parent

SQL = """
with corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null
                   or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
fperiod as (
  select database_id, start_on, end_on
  from goodparty_data_catalog.dbt.int__ballotready_filing_period
),
fp_ids as (
  select r.database_id as race_database_id,
         max(fp.databaseid) as filing_period_database_id
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  lateral view explode(r.filing_periods) as fp
  group by r.database_id
),
race_general as (
  select r.database_id  as br_race_id,
         r.position.databaseid as br_position_id,
         e.election_day
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e
    on r.election.databaseid = e.database_id
  where not coalesce(r.is_primary, false)
    and not coalesce(r.is_runoff, false)
    and not coalesce(r.is_recall, false)
),
race_deadline as (
  select rg.br_position_id, rg.election_day,
         max(p.end_on)   as filing_deadline,
         min(p.start_on) as filing_window_start
  from race_general rg
  left join fp_ids fi on fi.race_database_id = rg.br_race_id
  left join fperiod p on p.database_id = fi.filing_period_database_id
  group by 1, 2
),
-- BR arrivals per race: how much of the race's eventual BR roster had landed by the
-- filing deadline. Only the by-deadline count is feature-eligible (PIT-safe);
-- the total is diagnostics only.
br_arrivals as (
  select rg.br_position_id, rg.election_day,
         count(*) as n_br_candidacies_total,
         sum(case when rd.filing_deadline is not null
                   and cast(cv.candidacy_created_at as date) <= rd.filing_deadline
                  then 1 else 0 end) as n_br_candidacies_by_deadline
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv
  join race_general rg on cv.br_race_id = cast(rg.br_race_id as string)
  left join race_deadline rd
    on rd.br_position_id = rg.br_position_id and rd.election_day = rg.election_day
  group by 1, 2
),
-- The subject's OWN BR candidacy, reached by the same join path as br_arrivals so
-- membership in that aggregate is guaranteed and the subtraction can never go negative.
self_arrivals as (
  select cs.gp_candidacy_id, rg.br_position_id, rg.election_day,
         min(cast(cv.candidacy_created_at as date)) as self_created_on
  from goodparty_data_catalog.mart_civics.candidacy_stage cs
  join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv
    on cast(cv.br_candidacy_id as string) = cast(cs.br_candidacy_id as string)
  join race_general rg on cv.br_race_id = cast(rg.br_race_id as string)
  where cs.br_candidacy_id is not null
  group by 1, 2, 3
)
select
  u.user_id,
  u.campaign_id,
  c.gp_candidacy_id,
  c.gp_candidate_id,
  coalesce(k.corroborated, 0)                        as corroborated,
  c.general_election_date,
  rd.filing_deadline,
  rd.filing_window_start,
  cast(u.user_created_at as date)                    as signup_date,
  lower(regexp_extract(u.user_email, '@(.+)$', 1))   as email_domain,
  u.user_first_name,
  u.user_last_name,
  u.user_zip,
  u.campaign_state,
  u.election_level,
  u.office_type,
  u.br_normalized_position_type,
  u.campaign_party,
  u.voter_count,
  cast(u.ballotready_position_id as bigint)          as br_position_id,
  u.icp_office_win,
  u.is_pro,
  ba.n_br_candidacies_total,
  case when sa.self_created_on is not null
        and rd.filing_deadline is not null
        and sa.self_created_on <= rd.filing_deadline
       then 1 else 0 end                            as self_in_arrivals,
  ba.n_br_candidacies_by_deadline
    - case when sa.self_created_on is not null
            and rd.filing_deadline is not null
            and sa.self_created_on <= rd.filing_deadline
           then 1 else 0 end                         as n_br_candidacies_by_deadline
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
join goodparty_data_catalog.mart_civics.candidacy c
  on u.campaign_id = c.product_campaign_id
 and u.election_date = c.general_election_date
left join corrob k on c.gp_candidacy_id = k.gp_candidacy_id
left join race_deadline rd
  on rd.br_position_id = cast(u.ballotready_position_id as bigint)
 and rd.election_day = c.general_election_date
left join br_arrivals ba
  on ba.br_position_id = cast(u.ballotready_position_id as bigint)
 and ba.election_day = c.general_election_date
left join self_arrivals sa
  on sa.gp_candidacy_id = c.gp_candidacy_id
 and sa.br_position_id = cast(u.ballotready_position_id as bigint)
 and sa.election_day = c.general_election_date
where u.is_latest_version
  and not u.is_demo
  and c.general_election_date between date'{start}' and date'{end}'
  and c.general_election_date {completion} current_date()
"""

# The training set needs completed elections, because a row's label is only observable
# once its election has happened. The SCORING population is the exact complement, and it
# shares this SQL on purpose: the joins and the PIT gates must not drift between the rows
# a model learns from and the rows it scores. See build_scoring_population.py.
COMPLETED = "<"
UPCOMING = ">="

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default="2050-01-01")
    ap.add_argument("--out", default="working_set.parquet")
    args = ap.parse_args()

    df = run_query(SQL.format(start=args.start, end=args.end, completion=COMPLETED))
    print(f"rows: {len(df):,}  users: {df.user_id.nunique():,}  positive rate: {df.corroborated.mean():.3f}")
    print(f"deadline present: {df.filing_deadline.notna().mean():.1%}")
    print(f"window start present: {df.filing_window_start.notna().mean():.1%}")
    print(f"dupe campaign rows: {df.campaign_id.duplicated().sum()}")

    # Opponents-only receipts. The subtraction is bounded by construction (self is a
    # member of the aggregate it is removed from), so a negative count means the join
    # path drifted and the number is not opponents-only any more.
    n_bd = df["n_br_candidacies_by_deadline"]
    assert (n_bd.dropna() >= 0).all(), "negative arrivals count: self-subtraction is unsound"
    print(f"self subtracted: {df.self_in_arrivals.sum():,} rows " f"({df.self_in_arrivals.mean():.1%})")
    print(f"  corroborated among them: {df.loc[df.self_in_arrivals.eq(1), 'corroborated'].mean():.1%}")
    print(f"  vs base rate:            {df.corroborated.mean():.1%}")
    (HERE / "datasets").mkdir(exist_ok=True)
    out = HERE / "datasets" / args.out
    df.to_parquet(out, index=False)
    print(f"saved -> {out}")
