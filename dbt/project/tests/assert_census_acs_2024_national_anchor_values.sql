{{ config(severity="error") }}

-- Anchor pins, the decennial frame-totals pattern: the staged national row (and
-- one spot state) must equal the PUBLISHED 2020-2024 ACS values, which are
-- independent of the loaded files (retrieved from the Census API on 2026-07-20
-- and cross-checked equal to the loaded raw rows before pinning). Guards a
-- silent source revision, a partial re-load, and the staging parse path itself
-- (these values pass through GEO_ID parsing, jam mapping, and casting). The
-- 2020-2024 release is static, so fixed constants are appropriate here, exactly
-- like the decennial test; do not copy this pattern to refreshing sources.
-- households_income_universe must also equal households: two tables publishing
-- the same universe.
with
    staged as (
        select
            max(
                case when summary_level = '010' then total_population end
            ) as us_total_population,
            max(case when summary_level = '010' then households end) as us_households,
            max(
                case when summary_level = '010' then aggregate_household_income end
            ) as us_aggregate_household_income,
            max(
                case when summary_level = '010' then median_household_income end
            ) as us_median_household_income,
            max(
                case when summary_level = '010' then median_home_value end
            ) as us_median_home_value,
            max(
                case when summary_level = '010' then households_income_universe end
            ) as us_households_income_universe,
            max(
                case
                    when summary_level = '040' and geoid = '06' then total_population
                end
            ) as ca_total_population
        from {{ ref("stg_census_acs__geo_estimates") }}
    )

select *
from staged
where
    us_total_population is distinct from 334922499
    or us_households is distinct from 129227496
    or us_aggregate_household_income is distinct from 14671199681000
    or us_median_household_income is distinct from 80734
    or us_median_home_value is distinct from 332700
    or us_households_income_universe is distinct from us_households
    or ca_total_population is distinct from 39287377
