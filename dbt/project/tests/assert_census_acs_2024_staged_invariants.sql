{{ config(severity="error") }}

-- All-row semantic invariants over the staged model: cheap identities that a
-- wrong cell wiring, a broken jam mapping, or a bad join would violate
-- somewhere in 290k+ geographies. Universe equalities are exact (the Census
-- publishes these tables internally consistent); component sums are exact
-- counts, checked where every component is present (suppression nulls a
-- component, and a partially suppressed identity is unknowable); bounds
-- require numerator <= universe wherever both sides are present. Each arm is
-- labeled for diagnosis.
with
    staged as (select * from {{ ref("stg_census_acs__geo_estimates") }}),

    violations as (
        select
            case
                when population_race_universe is distinct from total_population
                then 'race_universe_not_total_population'
                when households_broadband_universe is distinct from households
                then 'broadband_universe_not_households'
                when households_income_universe is distinct from households
                then 'income_universe_not_households'
                when
                    population_white_nh is not null
                    and population_black_nh is not null
                    and population_native_american_nh is not null
                    and population_asian_nh is not null
                    and population_other_nh is not null
                    and population_hispanic is not null
                    and population_race_universe is not null
                    and population_white_nh
                    + population_black_nh
                    + population_native_american_nh
                    + population_asian_nh
                    + population_other_nh
                    + population_hispanic
                    != population_race_universe
                then 'race_components_do_not_sum_to_universe'
                when
                    households_income_under_10k is not null
                    and households_income_10k_to_15k is not null
                    and households_income_15k_to_20k is not null
                    and households_income_20k_to_25k is not null
                    and households_income_25k_to_30k is not null
                    and households_income_30k_to_35k is not null
                    and households_income_35k_to_40k is not null
                    and households_income_40k_to_45k is not null
                    and households_income_45k_to_50k is not null
                    and households_income_50k_to_60k is not null
                    and households_income_60k_to_75k is not null
                    and households_income_75k_to_100k is not null
                    and households_income_100k_to_125k is not null
                    and households_income_125k_to_150k is not null
                    and households_income_150k_to_200k is not null
                    and households_income_200k_plus is not null
                    and households_income_universe is not null
                    and households_income_under_10k
                    + households_income_10k_to_15k
                    + households_income_15k_to_20k
                    + households_income_20k_to_25k
                    + households_income_25k_to_30k
                    + households_income_30k_to_35k
                    + households_income_35k_to_40k
                    + households_income_40k_to_45k
                    + households_income_45k_to_50k
                    + households_income_50k_to_60k
                    + households_income_60k_to_75k
                    + households_income_75k_to_100k
                    + households_income_100k_to_125k
                    + households_income_125k_to_150k
                    + households_income_150k_to_200k
                    + households_income_200k_plus
                    != households_income_universe
                then 'income_brackets_do_not_sum_to_universe'
                when
                    population_under_18 is not null
                    and population_65_plus is not null
                    and total_population is not null
                    and population_under_18 + population_65_plus > total_population
                then 'age_brackets_exceed_total_population'
                when
                    population_hs_or_higher is not null
                    and population_25_plus is not null
                    and population_hs_or_higher > population_25_plus
                then 'hs_or_higher_exceeds_25_plus'
                when
                    population_bachelors_or_higher is not null
                    and population_hs_or_higher is not null
                    and population_bachelors_or_higher > population_hs_or_higher
                then 'bachelors_exceeds_hs_or_higher'
                when
                    population_25_plus is not null
                    and total_population is not null
                    and population_25_plus > total_population
                then '25_plus_exceeds_total_population'
                when
                    unemployed_population is not null
                    and civilian_labor_force is not null
                    and unemployed_population > civilian_labor_force
                then 'unemployed_exceeds_civilian_labor_force'
                when
                    civilian_labor_force is not null
                    and total_population is not null
                    and civilian_labor_force > total_population
                then 'civilian_labor_force_exceeds_total_population'
                when
                    population_below_poverty is not null
                    and population_poverty_universe is not null
                    and population_below_poverty > population_poverty_universe
                then 'below_poverty_exceeds_poverty_universe'
                when
                    population_poverty_universe is not null
                    and total_population is not null
                    and population_poverty_universe > total_population
                then 'poverty_universe_exceeds_total_population'
                when
                    owner_occupied_households is not null
                    and households is not null
                    and owner_occupied_households > households
                then 'owner_occupied_exceeds_households'
                when
                    households_with_broadband is not null
                    and households_broadband_universe is not null
                    and households_with_broadband > households_broadband_universe
                then 'broadband_exceeds_universe'
                when
                    population_in_households is not null
                    and total_population is not null
                    and population_in_households > total_population
                then 'population_in_households_exceeds_total'
                when
                    population_in_households is not null
                    and households is not null
                    and population_in_households < households
                then 'population_in_households_below_household_count'
            end as violation,
            summary_level,
            geoid
        from staged
    )

select violation, summary_level, geoid
from violations
where violation is not null
