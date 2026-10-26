-- ACS 2020-2024 5-year counts per district, keyed like district_census_stats: one row
-- per (state_postal_code, district_type, district_name) for the allocation's local
-- districts, plus one statewide row per state read straight from the published state
-- estimates.
--
-- Local rows allocate block-group estimates through the district population
-- allocation: a block group gives a district the share of its 2020 census population
-- that the allocation places in that district. The denominator is the whole block
-- group, zero-voter blocks included, so local rows carry the same zero-voter-block
-- undercount as district_population.
--
-- Only additive counts are allocated. Medians cannot be, margins of error are not
-- propagated, and aggregate household income is left out because Census suppresses
-- it in some block groups and a sum would quietly undercount.
{%- set count_columns = [
    "total_population",
    "population_under_18",
    "population_65_plus",
    "population_male_18_plus",
    "population_male_18_to_24",
    "population_male_25_to_34",
    "population_male_35_to_44",
    "population_male_45_to_54",
    "population_male_55_to_64",
    "population_male_65_plus",
    "population_female_18_plus",
    "population_female_18_to_24",
    "population_female_25_to_34",
    "population_female_35_to_44",
    "population_female_45_to_54",
    "population_female_55_to_64",
    "population_female_65_plus",
    "population_race_universe",
    "population_white_nh",
    "population_black_nh",
    "population_native_american_nh",
    "population_asian_nh",
    "population_other_nh",
    "population_hispanic",
    "population_25_plus",
    "population_hs_or_higher",
    "population_bachelors_or_higher",
    "civilian_labor_force",
    "unemployed_population",
    "households",
    "owner_occupied_households",
    "population_in_households",
    "households_broadband_universe",
    "households_with_broadband",
    "population_poverty_universe",
    "population_below_poverty",
    "households_income_universe",
    "households_income_under_10k",
    "households_income_10k_to_15k",
    "households_income_15k_to_20k",
    "households_income_20k_to_25k",
    "households_income_25k_to_30k",
    "households_income_30k_to_35k",
    "households_income_35k_to_40k",
    "households_income_40k_to_45k",
    "households_income_45k_to_50k",
    "households_income_50k_to_60k",
    "households_income_60k_to_75k",
    "households_income_75k_to_100k",
    "households_income_100k_to_125k",
    "households_income_125k_to_150k",
    "households_income_150k_to_200k",
    "households_income_200k_plus",
] %}

with
    acs as (select * from {{ ref("stg_census_acs__geo_estimates") }}),

    ct_block_groups as (
        select * from {{ ref("stg_census_acs__ct_block_planning_region") }}
    ),

    blocks as (
        select
            block_population.block_geoid,
            coalesce(
                ct_block_groups.acs_block_group_geoid,
                left(block_population.block_geoid, 12)
            ) as block_group_geoid,
            block_population.population
        from {{ ref("stg_census__block_population") }} as block_population
        left join
            ct_block_groups
            on block_population.block_geoid = ct_block_groups.block_geoid
    ),

    block_group_population as (
        select block_group_geoid, sum(population) as block_group_population
        from blocks
        group by block_group_geoid
    ),

    district_block_groups as (
        select
            allocation.state_postal_code,
            allocation.district_type,
            allocation.district_name,
            blocks.block_group_geoid,
            sum(allocation.allocated_population) as allocated_population
        from {{ ref("int__district_census_allocation") }} as allocation
        inner join blocks on allocation.block_geoid = blocks.block_geoid
        group by
            allocation.state_postal_code,
            allocation.district_type,
            allocation.district_name,
            blocks.block_group_geoid
    ),

    weights as (
        select
            district_block_groups.*,
            district_block_groups.allocated_population / nullif(
                block_group_population.block_group_population, 0
            ) as block_group_share
        from district_block_groups
        inner join
            block_group_population
            on district_block_groups.block_group_geoid
            = block_group_population.block_group_geoid
    ),

    local_districts as (
        select
            weights.state_postal_code,
            weights.district_type,
            weights.district_name,
            count(distinct weights.block_group_geoid) as n_census_block_groups,
            {%- for column in count_columns %}
                cast(
                    sum(weights.block_group_share * acs.{{ column }}) as double
                ) as {{ column }}
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        from weights
        inner join
            acs on acs.summary_level = '150' and weights.block_group_geoid = acs.geoid
        group by weights.state_postal_code, weights.district_type, weights.district_name
    ),

    state_block_group_counts as (
        select left(geoid, 2) as state_fips, count(*) as n_census_block_groups
        from acs
        where summary_level = '150'
        group by left(geoid, 2)
    ),

    statewide as (
        select
            states.place_name as state_postal_code,
            'State' as district_type,
            states.place_name as district_name,
            state_block_group_counts.n_census_block_groups,
            {%- for column in count_columns %}
                cast(acs.{{ column }} as double) as {{ column }}
                {%- if not loop.last %},{% endif %}
            {%- endfor %}
        from acs
        inner join
            {{ ref("fips_codes") }} as states
            on acs.geoid = states.fips_code
            and states.level = 'state'
        inner join
            state_block_group_counts on acs.geoid = state_block_group_counts.state_fips
        where acs.summary_level = '040'
    )

select *
from local_districts
union all
select *
from statewide
