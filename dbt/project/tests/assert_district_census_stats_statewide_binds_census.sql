-- Statewide binding (DATA-1994): each statewide (district_type='State') row's
-- district_population and n_census_blocks must equal the direct census rollup over
-- the 50 + DC fips_codes states (Puerto Rico excluded by the inner join). The 2020
-- census frame is static, so this is a fixed binding (TDD 7.3 -- fixed tests on the
-- immutable census frame). Catches FIPS-mapping errors, PR leakage, and dropped or
-- duplicated blocks in the statewide branch. Returns offending rows; empty = pass.
with
    census_rollup as (
        select
            sf.place_name as state_postal_code,
            cast(sum(p.population) as double) as census_pop,
            count(distinct p.block_geoid) as census_blocks
        from {{ ref("stg_census__block_population") }} p
        join
            {{ ref("fips_codes") }} sf
            on left(p.block_geoid, 2) = sf.fips_code
            and sf.level = 'state'
        group by sf.place_name
    ),

    statewide as (
        select state_postal_code, district_population, n_census_blocks
        from {{ ref("district_census_stats") }}
        where district_type = 'State'
    )

select coalesce(s.state_postal_code, c.state_postal_code) as state_postal_code
from statewide s
full outer join census_rollup c on s.state_postal_code = c.state_postal_code
where
    s.state_postal_code is null
    or c.state_postal_code is null
    or abs(s.district_population - c.census_pop) > 1e-6
    or s.n_census_blocks <> c.census_blocks
