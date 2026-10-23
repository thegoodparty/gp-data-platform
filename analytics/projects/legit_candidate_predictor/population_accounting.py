"""DATA-2225 review fix: population accounting + in-cycle feature availability.

Produces two CSVs the notebook embeds:
- population_accounting.csv : raw signups -> office-declared -> labeled pool census,
  incl. the loose-join vs version-aware-join reconciliation (the 766-user delta).
- incycle_feature_availability.csv : feature coverage on the forward scoring set.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from databricks_conn import run_query

HERE = Path(__file__).resolve().parent

CENSUS = """
with corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null
                   or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
base as (
  select u.user_id, u.is_demo,
         c.gp_candidacy_id,
         c.general_election_date,
         coalesce(k.corroborated, 0) as corroborated
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  left join goodparty_data_catalog.mart_civics.candidacy c
    on u.campaign_id = c.product_campaign_id
   and u.election_date = c.general_election_date
  left join corrob k on c.gp_candidacy_id = k.gp_candidacy_id
  where u.is_latest_version
)
select
  case
    when is_demo then '0_demo_account'
    when gp_candidacy_id is null then '1_no_office_declared'
    when general_election_date not between date'2020-01-01' and date'2050-01-01'
         then '2_corrupt_election_date'
    when general_election_date >= current_date() then '3_in_cycle_scoring_set'
    when general_election_date >= date'2025-01-01' then '4_labeled_pool_2025_plus'
    else '5_completed_pre_2025'
  end as segment,
  count(distinct user_id) as n_users,
  round(avg(corroborated), 3) as corroborated_rate
from base
group by 1 order by 1
"""

RECONCILE = """
with pool as (
  select u.user_id,
         max(case when c_loose.gp_candidacy_id is not null then 1 else 0 end)  as loose,
         max(case when c_strict.gp_candidacy_id is not null then 1 else 0 end) as strict
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  left join goodparty_data_catalog.mart_civics.candidacy c_loose
    on u.campaign_id = c_loose.product_campaign_id
   and c_loose.general_election_date between date'2025-01-01' and date'2050-01-01'
   and c_loose.general_election_date < current_date()
  left join goodparty_data_catalog.mart_civics.candidacy c_strict
    on u.campaign_id = c_strict.product_campaign_id
   and u.election_date = c_strict.general_election_date
   and c_strict.general_election_date between date'2025-01-01' and date'2050-01-01'
   and c_strict.general_election_date < current_date()
  where u.is_latest_version and not u.is_demo
  group by u.user_id
)
select
  sum(loose)                                   as n_loose_join_framing,
  sum(strict)                                  as n_version_aware_join,
  sum(case when loose = 1 and strict = 0 then 1 else 0 end) as n_dropped_by_election_date_match
from pool
"""

INCYCLE = """
with fperiod as (
  select database_id, end_on from goodparty_data_catalog.dbt.int__ballotready_filing_period
),
fp_ids as (
  select r.database_id as race_database_id, max(fp.databaseid) as filing_period_database_id
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  lateral view explode(r.filing_periods) as fp group by r.database_id
),
race_general as (
  select r.database_id as br_race_id, r.position.databaseid as br_position_id, e.election_day
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e
    on r.election.databaseid = e.database_id
  where not coalesce(r.is_primary, false) and not coalesce(r.is_runoff, false)
    and not coalesce(r.is_recall, false)
),
race_deadline as (
  select rg.br_position_id, rg.election_day, max(p.end_on) as filing_deadline
  from race_general rg
  left join fp_ids fi on fi.race_database_id = rg.br_race_id
  left join fperiod p on p.database_id = fi.filing_period_database_id
  group by 1, 2
),
br_any as (
  select rg.br_position_id, rg.election_day, count(*) as n_br
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv
  join race_general rg on cv.br_race_id = cast(rg.br_race_id as string)
  group by 1, 2
)
select
  count(distinct u.user_id) as n_users,
  round(avg(case when rd.filing_deadline is not null then 1.0 else 0.0 end), 3) as pct_deadline,
  round(avg(case when u.ballotready_position_id is not null then 1.0 else 0.0 end), 3) as pct_br_position_id,
  round(avg(case when u.br_normalized_position_type is not null then 1.0 else 0.0 end), 3) as pct_position_type,
  round(avg(case when u.campaign_party is not null then 1.0 else 0.0 end), 3) as pct_party,
  round(avg(case when ba.n_br is not null then 1.0 else 0.0 end), 3) as pct_br_arrivals_row,
  round(avg(case when u.voter_count is not null then 1.0 else 0.0 end), 3) as pct_voter_count
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
join goodparty_data_catalog.mart_civics.candidacy c
  on u.campaign_id = c.product_campaign_id
 and u.election_date = c.general_election_date
left join race_deadline rd
  on rd.br_position_id = cast(u.ballotready_position_id as bigint)
 and rd.election_day = c.general_election_date
left join br_any ba
  on ba.br_position_id = cast(u.ballotready_position_id as bigint)
 and ba.election_day = c.general_election_date
where u.is_latest_version and not u.is_demo
  and c.general_election_date between date'2020-01-01' and date'2050-01-01'
  and c.general_election_date >= current_date()
"""

if __name__ == "__main__":
    census = run_query(CENSUS)
    print(census.to_string(index=False))
    census.to_csv(HERE / "datasets" / "population_accounting.csv", index=False)

    rec = run_query(RECONCILE)
    print(rec.to_string(index=False))
    rec.to_csv(HERE / "datasets" / "join_reconciliation.csv", index=False)

    inc = run_query(INCYCLE)
    print(inc.to_string(index=False))
    inc.to_csv(HERE / "datasets" / "incycle_feature_availability.csv", index=False)
