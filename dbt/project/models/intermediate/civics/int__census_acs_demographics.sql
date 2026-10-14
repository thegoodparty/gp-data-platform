-- ACS demographic counts at the two grains the district layer reads: block group
-- (150), allocated into districts, and state (040), used whole for statewide rows.
-- Counts only, so every column stays additive under allocation. The published
-- medians are left out because a median cannot be allocated. Income is collapsed
-- from sixteen published brackets into the six bands the product shows; a band is
-- null when any of its brackets is suppressed, matching the staging rule.
-- The ACS codes Connecticut under planning regions while decennial blocks and L2
-- geocodes keep the 2020 county codes, so CT block groups are relabelled to 2020
-- codes (one-to-one at this grain) for every downstream prefix join to match. The
-- few the crosswalk cannot reach are unpopulated water.
with
    ct_block_groups as (
        select distinct
            left(block_geoid_2022, 12) as geoid_2022,
            left(block_geoid_2020, 12) as geoid_2020
        from {{ ref("stg_census_acs__ct_block_to_planning_region_2022") }}
    )

select
    summary_level,
    coalesce(ct_block_groups.geoid_2020, acs.geoid) as geoid,
    total_population,
    population_under_18,
    population_65_plus,
    population_race_universe,
    population_white_nh,
    population_black_nh,
    population_native_american_nh,
    population_asian_nh,
    population_other_nh,
    population_hispanic,
    population_25_plus,
    population_hs_or_higher,
    population_bachelors_or_higher,
    households,
    owner_occupied_households,
    population_poverty_universe,
    population_below_poverty,
    households_income_universe,
    households_income_under_10k
    + households_income_10k_to_15k
    + households_income_15k_to_20k
    + households_income_20k_to_25k as households_income_under_25k,
    households_income_25k_to_30k
    + households_income_30k_to_35k
    + households_income_35k_to_40k
    + households_income_40k_to_45k
    + households_income_45k_to_50k as households_income_25k_to_50k,
    households_income_50k_to_60k
    + households_income_60k_to_75k as households_income_50k_to_75k,
    households_income_75k_to_100k,
    households_income_100k_to_125k
    + households_income_125k_to_150k as households_income_100k_to_150k,
    households_income_150k_to_200k
    + households_income_200k_plus as households_income_150k_plus
from {{ ref("stg_census_acs__geo_estimates") }} as acs
left join ct_block_groups on acs.geoid = ct_block_groups.geoid_2022
where summary_level in ('150', '040')
