"""DATA-2225 MVP step 1: coarse verification before the working set.

1a. BR-raw filing-deadline coverage by election year (2024, 2025, 2026):
    share of general-election races with a recoverable filing deadline.
1b. Reconcile labeled-pool counts against the framing numbers
    (2024: 9,036 / 2025: 20,017 / completed 2026: 1,229).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
import pandas as pd
from databricks_conn import run_query

pd.set_option("display.width", 200)

# 1a. Deadline coverage at the race grain, via the DATA-2202 recipe.
DEADLINE_COVERAGE = """
with elec as (
  select database_id, name, election_day, year(election_day) as yr
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election
  where year(election_day) between 2024 and 2026
    and lower(name) not like '%primary%' and lower(name) not like '%runoff%'
),
fperiod as (
  select database_id, end_on
  from goodparty_data_catalog.dbt.int__ballotready_filing_period
),
fp_ids as (
  select r.database_id as race_database_id, max(fp.databaseid) as filing_period_database_id
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  lateral view explode(r.filing_periods) as fp
  group by r.database_id
)
select
  e.yr,
  count(*)                                        as n_races,
  sum(case when p.end_on is not null then 1 else 0 end) as n_with_deadline,
  round(100.0 * sum(case when p.end_on is not null then 1 else 0 end) / count(*), 1) as pct_with_deadline
from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
join elec e on r.election.databaseid = e.database_id
left join fp_ids on fp_ids.race_database_id = r.database_id
left join fperiod p on p.database_id = fp_ids.filing_period_database_id
group by e.yr
order by e.yr
"""

# 1b. Labeled-pool reconciliation (same query family as framing, condensed).
POOL_RECONCILE = """
with corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null
                   or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
)
select
  year(c.general_election_date) as cycle_year,
  count(distinct u.user_id) as n_users,
  round(avg(coalesce(k.corroborated, 0)), 3) as positive_rate
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
join goodparty_data_catalog.mart_civics.candidacy c
  on u.campaign_id = c.product_campaign_id
 and u.election_date = c.general_election_date
left join corrob k on c.gp_candidacy_id = k.gp_candidacy_id
where u.is_latest_version and not u.is_demo
  and c.general_election_date between date'2020-01-01' and date'2050-01-01'
  and c.general_election_date < current_date()
  and year(c.general_election_date) >= 2024
group by 1 order by 1
"""

print("=== 1a. BR-raw general filing-deadline coverage by year (race grain) ===")
print(run_query(DEADLINE_COVERAGE).to_string(index=False))

print("\n=== 1b. labeled pool reconcile (expect ~9,036 / ~20,017 / ~1,229) ===")
print(run_query(POOL_RECONCILE).to_string(index=False))
