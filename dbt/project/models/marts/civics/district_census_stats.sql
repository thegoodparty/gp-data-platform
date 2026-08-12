-- Civics mart district_census_stats: the universal district population reference.
-- One row per district -- (state_postal_code, district_type,
-- district_name) -- for every district of the allocation's curated office-bearing types
-- nationwide, PLUS one clearly-flagged statewide row per state (50 + DC).
--
-- Local rows come from int__district_population, the rollup of
-- int__district_census_allocation (THE DISTRICT POPULATION ALLOCATION). The allocation already
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
-- v1 = population columns only. ACS demographics + the validated (block-group)
-- non-exact-assignment share land in the demographic layer (phase 2).
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

    statewide as (
        select
            sp.state_postal_code,
            'State' as district_type,
            sp.state_postal_code as district_name,
            sp.district_population,
            sp.n_census_blocks,
            coalesce(sv.registered_voters, 0) as registered_voters
        from statewide_pop sp
        left join statewide_voters sv using (state_postal_code)
    )

select
    state_postal_code,
    district_type,
    district_name,
    district_population,
    n_census_blocks,
    registered_voters
from local_districts

union all

select
    state_postal_code,
    district_type,
    district_name,
    district_population,
    n_census_blocks,
    registered_voters
from statewide
