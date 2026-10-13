{%- set acs_counts = [
    "total_population",
    "population_under_18",
    "population_65_plus",
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
    "households",
    "owner_occupied_households",
    "population_poverty_universe",
    "population_below_poverty",
    "households_income_universe",
    "households_income_under_25k",
    "households_income_25k_to_50k",
    "households_income_50k_to_75k",
    "households_income_75k_to_100k",
    "households_income_100k_to_150k",
    "households_income_150k_plus",
] -%}

-- ACS demographics for each local district: block-group counts allocated by the
-- district's share of the block group's population. The share's denominator is the
-- block group's full 2020 census population, zero-voter blocks included, so a
-- block group that is only partly inside a district's type (a city edge, say)
-- gives the district only its part rather than all of it.
-- Shares come from 2020 decennial population and are applied to ACS 2020-2024
-- counts: a ratio, so the vintage mismatch moves magnitudes, not shares.
-- Margins are not carried; allocating them needs a variance model this layer does
-- not have, so these are point estimates.
with
    allocation as (select * from {{ ref("int__district_census_allocation") }}),

    block_group_population as (
        select
            left(block_geoid, 12) as block_group_geoid,
            sum(population) as block_group_population
        from {{ ref("stg_census__block_population") }}
        group by left(block_geoid, 12)
    ),

    district_block_groups as (
        select
            state_postal_code,
            district_type,
            district_name,
            block_group_geoid,
            sum(allocated_population) as allocated_population
        from allocation
        group by state_postal_code, district_type, district_name, block_group_geoid
    ),

    shares as (
        select
            district_block_groups.*,
            district_block_groups.allocated_population
            / block_group_population.block_group_population as block_group_share
        from district_block_groups
        inner join
            block_group_population
            on district_block_groups.block_group_geoid
            = block_group_population.block_group_geoid
        where block_group_population.block_group_population > 0
    ),

    acs as (
        select *
        from {{ ref("int__census_acs_demographics") }}
        where summary_level = '150'
    )

select
    shares.state_postal_code,
    shares.district_type,
    shares.district_name,
    {%- for column in acs_counts %}
        sum(shares.block_group_share * acs.{{ column }}) as acs_{{ column }},
    {%- endfor %}
    -- Share of the district's allocated population living in block groups with a
    -- published ACS total. Below 1 means suppressed block groups left counts out.
    coalesce(
        sum(
            case
                when acs.total_population is not null then shares.allocated_population
            end
        )
        / nullif(sum(shares.allocated_population), 0),
        0
    ) as acs_coverage_share
from shares
left join acs on shares.block_group_geoid = acs.geoid
group by shares.state_postal_code, shares.district_type, shares.district_name
