-- int__district_population: the local-district rollup of the allocation, one
-- row per (state_postal_code, district_type, district_name). A pure GROUP BY over
-- int__district_census_allocation, which already conserves mass per (block, type),
-- so district_population is the block-population-weighted district total. It is
-- FRACTIONAL by design; round for display downstream.
--
-- Local (curated allocated) types only. Statewide rows carry a different
-- population basis (EXACT whole-state census, including zero-voter blocks) and
-- stay in district_census_stats, which unions this rollup with that branch.
--
-- Extracted out of that mart so district-grain consumers in the intermediate
-- layer (ICP office sizing) can join population without an intermediate model
-- reaching up into a mart.
--
-- Read ONE district_type at a time: summing across types double-counts, since
-- each type independently tiles the country.
select
    state_postal_code,
    district_type,
    district_name,
    cast(sum(allocated_population) as double) as district_population,
    -- count(distinct): unambiguously "distinct census blocks". The allocation's
    -- tested (block, type, name) key makes this == count(*), but distinct is
    -- self-documenting.
    count(distinct block_geoid) as n_census_blocks,
    sum(voters_in_block_district) as registered_voters
from {{ ref("int__district_census_allocation") }}
group by state_postal_code, district_type, district_name
