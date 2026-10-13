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

-- Civics mart district_census_stats: the universal district population reference.
-- One row per district -- (state_postal_code, district_type,
-- district_name) -- for every district of the allocation's curated office-bearing types
-- nationwide, PLUS one clearly-flagged statewide row per state (50 + DC).
--
-- Local rows come from int__district_population, the rollup of
-- int__district_census_allocation (THE DISTRICT POPULATION ALLOCATION). The
-- allocation already
-- conserves mass (allocations sum to block population per (block, type)), so
-- district_population = sum(allocated_population) is the block-population-weighted
-- district total. It is FRACTIONAL by design (exact mass conservation; round for
-- display downstream) and cast to double to pin the mart's column type across the
-- UNION with the integer-valued statewide branch.
--
-- TWO POPULATION BASES live in district_population, distinguished by district_type:
-- * local rows (the curated types): voter-block-ALLOCATED population -- inferred from
-- blocks that carry >=1 L2 voter, so they carry the documented ~2.4%
-- zero-voter-block undercount.
-- * statewide rows (district_type='State'): EXACT whole-state 2020 census
-- population (sum over ALL blocks incl. zero-voter), computed directly from
-- census -- a genuinely statewide official represents the whole state.
-- Reproduces the official 2020 frame to the person.
-- Consumers wanting only local districts filter WHERE district_type <> 'State'
-- (people_served excludes statewide from the North Star; the allocation-binding
-- test is likewise scoped to non-statewide rows).
--
-- This table is read ONE district_type at a time: summing district_population
-- across types double-counts (each type independently tiles the country), and
-- summing local + statewide mixes the two bases. That is inherent to a universal
-- district reference, not specific to the statewide rows.
--
-- ACS demographics (acs_* columns) are allocated block-group counts for local
-- rows (int__district_census_demographics) and the published state row for
-- statewide rows. The validated non-exact-assignment share is still to come.
with
    allocation as (select * from {{ ref("int__district_census_allocation") }}),

    -- the local (allocated-type) districts: a pure rollup of the allocation, which
    -- lives in int__district_population so district-grain consumers in the
    -- intermediate layer can join population without reading a mart. Exact-binds
    -- to the allocation (assert_district_census_stats_binds_allocation).
    local_districts as (select * from {{ ref("int__district_population") }}),

    -- statewide population: EXACT whole-state 2020 census total via the SAME
    -- geoid-FIPS -> fips_codes derivation the allocation uses for state_postal_code,
    -- so the postal-code domain matches the local rows exactly. The inner join to
    -- the (50 + DC) fips_codes state seed excludes Puerto Rico (no L2 coverage)
    -- and every territory.
    statewide_pop as (
        select
            sf.place_name as state_postal_code,
            cast(sum(p.population) as double) as district_population,
            count(distinct p.block_geoid) as n_census_blocks
        from {{ ref("stg_census__block_population") }} p
        join
            {{ ref("fips_codes") }} sf
            on left(p.block_geoid, 2) = sf.fips_code
            and sf.level = 'state'
        group by sf.place_name
    ),

    -- state registered-voter total: per block take the max voter count across the
    -- allocated types, then sum per state. Verified to equal the universal-County voter
    -- total to the voter (County covers every voter block), so this is the state's
    -- L2 voter count, robust for DC / independent cities that lack a County row.
    statewide_voters as (
        select state_postal_code, sum(block_voters) as registered_voters
        from
            (
                select
                    state_postal_code, block_geoid, max(voters_in_block) as block_voters
                from allocation
                group by state_postal_code, block_geoid
            )
        group by state_postal_code
    ),

    statewide_acs as (
        select
            sf.place_name as state_postal_code,
            {%- for column in acs_counts %}
                cast(acs.{{ column }} as double) as acs_{{ column }},
            {%- endfor %}
            cast(
                case when acs.total_population is not null then 1 else 0 end as double
            ) as acs_coverage_share
        from {{ ref("int__census_acs_demographics") }} acs
        join
            {{ ref("fips_codes") }} sf
            on acs.geoid = sf.fips_code
            and sf.level = 'state'
        where acs.summary_level = '040'
    ),

    statewide as (
        select
            sp.state_postal_code,
            'State' as district_type,
            sp.state_postal_code as district_name,
            sp.district_population,
            sp.n_census_blocks,
            coalesce(sv.registered_voters, 0) as registered_voters,
            {%- for column in acs_counts %} sa.acs_{{ column }},{% endfor %}
            coalesce(sa.acs_coverage_share, 0) as acs_coverage_share
        from statewide_pop sp
        left join statewide_voters sv using (state_postal_code)
        left join statewide_acs sa using (state_postal_code)
    ),

    local_with_demographics as (
        select
            ld.state_postal_code,
            ld.district_type,
            ld.district_name,
            ld.district_population,
            ld.n_census_blocks,
            ld.registered_voters,
            {%- for column in acs_counts %} dd.acs_{{ column }},{% endfor %}
            coalesce(dd.acs_coverage_share, 0) as acs_coverage_share
        from local_districts ld
        left join
            {{ ref("int__district_census_demographics") }} dd using (
                state_postal_code, district_type, district_name
            )
    )

select
    state_postal_code,
    district_type,
    district_name,
    district_population,
    n_census_blocks,
    registered_voters,
    {%- for column in acs_counts %} acs_{{ column }},{% endfor %}
    acs_coverage_share
from local_with_demographics

union all

select
    state_postal_code,
    district_type,
    district_name,
    district_population,
    n_census_blocks,
    registered_voters,
    {%- for column in acs_counts %} acs_{{ column }},{% endfor %}
    acs_coverage_share
from statewide
