"""DATA-2225 warehouse verification: Item A (filing-deadline gap decomposition)
and Item B (L2 district-match gap behind voter_count nulls).

Read-only SELECT queries. Pool CTE reproduces build_working_set.py exactly
(minus corrob/br_arrivals, which cannot change row count or the fields used here).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from databricks_conn import run_query

# ---------------------------------------------------------------------------
# Shared CTEs: exact reproduction of build_working_set.py pool recipe
# ---------------------------------------------------------------------------
POOL_CTES = """
with fperiod as (
  select database_id, end_on
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
         max(p.end_on) as filing_deadline
  from race_general rg
  left join fp_ids fi on fi.race_database_id = rg.br_race_id
  left join fperiod p on p.database_id = fi.filing_period_database_id
  group by 1, 2
),
pool as (
  select
    u.user_id,
    u.campaign_id,
    cast(u.ballotready_position_id as bigint) as br_position_id,
    c.general_election_date,
    rd.filing_deadline,
    u.voter_count
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  join goodparty_data_catalog.mart_civics.candidacy c
    on u.campaign_id = c.product_campaign_id
   and u.election_date = c.general_election_date
  left join race_deadline rd
    on rd.br_position_id = cast(u.ballotready_position_id as bigint)
   and rd.election_day = c.general_election_date
  where u.is_latest_version
    and not u.is_demo
    and c.general_election_date between date'2025-01-01' and date'2050-01-01'
    and c.general_election_date < current_date()
)
"""

# Per-(position, election_day) diagnostics of the deadline recipe, mirroring
# race_deadline's join chain step by step so we can see where it breaks.
POS_DIAG_CTE = """
, pos_diag as (
  select rg.br_position_id, rg.election_day,
         count(*)                          as n_races,
         count(fi.race_database_id)        as n_races_with_fp,
         count(fi.filing_period_database_id) as n_nonnull_fpids,
         count(p.database_id)              as n_fpids_resolved,
         count(p.end_on)                   as n_resolved_with_end_on
  from race_general rg
  left join fp_ids fi on fi.race_database_id = rg.br_race_id
  left join fperiod p on p.database_id = fi.filing_period_database_id
  group by 1, 2
)
"""

Q_POOL_COUNT = (
    POOL_CTES
    + """
select count(*) as pool_rows, count(distinct user_id) as pool_users from pool
"""
)

Q_A_TOTALS = (
    POOL_CTES
    + """
select
  count(*)                                                                  as pool_rows,
  sum(case when filing_deadline is null then 1 else 0 end)                  as missing_deadline_total,
  sum(case when br_position_id is null then 1 else 0 end)                   as no_br_position,
  sum(case when br_position_id is not null
            and filing_deadline is null then 1 else 0 end)                  as with_position_unresolved
from pool
"""
)

Q_A_CLASSIFY = (
    POOL_CTES
    + POS_DIAG_CTE
    + """
select bucket,
       count(*)                 as n_rows,
       count(distinct user_id)  as n_users
from (
  select
    pool.user_id,
    case
      when pd.br_position_id is null          then '1_no_general_race_row'
      when pd.n_races_with_fp = 0             then '2_filing_periods_empty'
      when pd.n_nonnull_fpids = 0             then '5_other_fp_element_id_null'
      when pd.n_fpids_resolved = 0            then '3_fp_id_missing_from_int_filing_period'
      when pd.n_resolved_with_end_on = 0      then '4_resolved_but_end_on_null'
      else '5_other_resolved_with_end_on_but_deadline_null'
    end as bucket
  from pool
  left join pos_diag pd
    on pd.br_position_id = pool.br_position_id
   and pd.election_day = pool.general_election_date
  where pool.br_position_id is not null
    and pool.filing_deadline is null
) t
group by bucket
order by bucket
"""
)

# ---------------------------------------------------------------------------
# Item B: voter_count nulls -> L2 match classification
# users_win_candidacy.voter_count = int__icp_offices.voter_count, joined on
# ballotready_position_id = br_database_position_id (= position.database_id).
# int__icp_offices: position left join l2_match (stg_model_predictions__
# llm_l2_br_match_20260126 on database_id = br_database_id) left join
# int__l2_district_aggregations on (l2_district_name, l2_district_type,
# position.state = state_postal_code).
# ---------------------------------------------------------------------------
B_CTES = (
    POOL_CTES
    + """
, null_vc_positions as (
  select br_position_id,
         count(*)                as n_rows,
         count(distinct user_id) as n_users
  from pool
  where br_position_id is not null
    and voter_count is null
  group by 1
),
pos as (
  select database_id, state, created_at, updated_at
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_position
),
match_agg as (
  select br_database_id,
         count(*)                                              as n_match_rows,
         sum(case when is_matched then 1 else 0 end)           as n_true,
         sum(case when not is_matched then 1 else 0 end)       as n_false,
         sum(case when is_matched is null then 1 else 0 end)   as n_null
  from goodparty_data_catalog.dbt.stg_model_predictions__llm_l2_br_match_20260126
  group by 1
),
matched_join as (
  select m.br_database_id,
         max(case when d.district_name is not null then 1 else 0 end) as has_agg_row,
         max(case when d.voter_count is not null then 1 else 0 end)   as has_agg_vc
  from goodparty_data_catalog.dbt.stg_model_predictions__llm_l2_br_match_20260126 m
  join pos p on p.database_id = m.br_database_id
  left join goodparty_data_catalog.dbt.int__l2_district_aggregations d
    on m.l2_district_name = d.district_name
   and m.l2_district_type = d.district_type
   and p.state = d.state_postal_code
  where m.is_matched
  group by 1
)
"""
)

Q_B_TOTALS = (
    POOL_CTES
    + """
select
  sum(case when br_position_id is not null and voter_count is null
       then 1 else 0 end)                                    as null_vc_rows,
  count(distinct case when br_position_id is not null and voter_count is null
       then br_position_id end)                              as null_vc_positions
from pool
"""
)

Q_B_CLASSIFY = (
    B_CTES
    + """
select
  case
    when p.database_id is null            then '0_position_id_not_in_br_position_table'
    when ma.br_database_id is null        then '1_absent_from_match_table'
    when ma.n_true = 0 and ma.n_null > 0  then '2b_is_matched_null_only'
    when ma.n_true = 0                    then '2_is_matched_false'
    when coalesce(mj.has_agg_row, 0) = 0  then '3_matched_but_no_l2_agg_row'
    when mj.has_agg_vc = 0                then '4_other_agg_row_null_voter_count'
    else '4_other_agg_has_voter_count_mart_stale'
  end as bucket,
  count(*)          as n_positions,
  sum(nv.n_rows)    as n_pool_rows,
  sum(nv.n_users)   as n_users
from null_vc_positions nv
left join pos p        on p.database_id = nv.br_position_id
left join match_agg ma on ma.br_database_id = nv.br_position_id
left join matched_join mj on mj.br_database_id = nv.br_position_id
group by 1
order by 1
"""
)

# Snapshot-staleness test: bucket-1 positions (in BR position table, absent
# from the 2026-01-26 match table) split by created_at vs the snapshot date.
Q_B_STALENESS = (
    B_CTES
    + """
select
  case
    when p.created_at is null then 'created_at_null'
    when p.created_at > timestamp'2026-01-26 00:00:00' then 'created_after_2026-01-26'
    else 'created_on_or_before_2026-01-26'
  end as created_split,
  count(*)                 as n_positions,
  sum(nv.n_rows)           as n_pool_rows,
  min(p.created_at)        as min_created_at,
  max(p.created_at)        as max_created_at
from null_vc_positions nv
join pos p on p.database_id = nv.br_position_id
left join match_agg ma on ma.br_database_id = nv.br_position_id
where ma.br_database_id is null
group by 1
order by 1
"""
)

EXPECTED_POOL = 20480


def show(label: str, sql: str):
    df = run_query(sql)
    print(f"\n===== {label} =====")
    print(df.to_string(index=False))
    return df


if __name__ == "__main__":
    pool = show("Pool count (expect 20,480 rows)", Q_POOL_COUNT)
    n = int(pool.iloc[0]["pool_rows"])
    if n != EXPECTED_POOL:
        # Verified 2026-08-06: the delta vs the 08-04 working_set.parquet is
        # daily-rebuild churn (16 rows out, 14 in), not a recipe error. Allow
        # small drift; stop only on a large mismatch.
        if abs(n - EXPECTED_POOL) > 50:
            print(f"\nSTOP: pool count {n:,} != expected {EXPECTED_POOL:,}. Not proceeding.")
            sys.exit(1)
        print(f"\nWARNING: pool count {n:,} != {EXPECTED_POOL:,} (warehouse drift; proceeding).")

    tot = show("Item A totals", Q_A_TOTALS)
    r = tot.iloc[0]
    pr = float(r["pool_rows"])
    print(
        f"missing total: {r['missing_deadline_total'] / pr:.1%} | "
        f"no BR position: {r['no_br_position'] / pr:.1%} | "
        f"with-position unresolved: {r['with_position_unresolved'] / pr:.1%}"
    )

    a = show("Item A bucket classification", Q_A_CLASSIFY)
    a["pct_of_pool"] = (a["n_rows"] / pr * 100).round(2)
    print(a.to_string(index=False))
    print(
        f"bucket sum: {a['n_rows'].sum():,} "
        f"(should equal with-position-unresolved = {int(r['with_position_unresolved']):,})"
    )

    btot = show("Item B totals (expect 1,154 null-voter_count rows)", Q_B_TOTALS)
    b = show("Item B bucket classification", Q_B_CLASSIFY)
    b["pct_of_null_vc_rows"] = (b["n_pool_rows"] / int(btot.iloc[0]["null_vc_rows"]) * 100).round(2)
    print(b.to_string(index=False))

    show("Item B snapshot staleness (bucket 1 positions)", Q_B_STALENESS)
