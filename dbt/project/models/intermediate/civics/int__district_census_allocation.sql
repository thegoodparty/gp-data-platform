{{ config(materialized="table", tags=["intermediate", "civics", "substrate"]) }}

-- int__district_census_allocation: THE SUBSTRATE (DATA-1992). One row per
-- (census block, district_type, normalized district_name) carrying the 2020
-- decennial census population allocated to that district within that block.
-- The split fraction is the within-(block, district_type) L2 voter share
-- (pct_of_geo); the magnitude is decennial block population, NEVER voter
-- counts. Every consumer (people_served, official_constituents,
-- district_census_stats) reads this table.
--
-- Block grain ONLY. block_group_geoid (LEFT 12) and tract_geoid (LEFT 11) are
-- carried so consumers roll up with a GROUP BY; coarser grains are never
-- stored as rows (no cross-grain double-count). Counts only, PII-free.
-- Depends only on L2 + census -- no election-api edge (that lives downstream
-- in int__serve_district_resolution).
with
    block_district_map as (select * from {{ ref("int__l2_block_district_map") }}),

    block_population as (
        select block_geoid, population as block_population
        from {{ ref("stg_census__block_population") }}
    ),

    joined as (
        select
            m.block_geoid,
            left(m.block_geoid, 12) as block_group_geoid,
            left(m.block_geoid, 11) as tract_geoid,
            m.state_postal_code,
            m.district_type,
            m.district_name,
            m.voters_in_block_district,
            m.voters_in_block,
            p.block_population,
            -- split fraction = within-(block, district_type) voter share. At
            -- block grain this is the fraction of the block allocated here.
            m.voters_in_block_district * 1.0 / m.voters_in_block as pct_of_geo,
            -- is_non_exact: the block's L2 voters report >1 district of this
            -- type. This is the voter-OBSERVED split at block grain -- it
            -- includes L2 geocoding noise and misses geographic splits no
            -- observed voter falls on both sides of, so it is an indicator, not
            -- a precise geographic-overlap measure. The validated per-district
            -- non-exact-assignment SHARE (the accuracy story) is computed at
            -- block-group grain in the demographic layer (phase 2), where
            -- splitting is real; at block grain it is mostly noise (TDD App B.2).
            count(*) over (partition by m.block_geoid, m.district_type)
            > 1 as is_non_exact,
            m.loaded_at
        -- inner join: blocks with L2 voters but no 2020 census-frame row
        -- (~0.13%, geocoding artifacts) cannot be allocated and are dropped;
        -- the match rate is canaried (assert_..._census_match_floor) and
        -- reconciled in the PR.
        from block_district_map m
        join block_population p on m.block_geoid = p.block_geoid
    )

select
    block_geoid,
    block_group_geoid,
    tract_geoid,
    state_postal_code,
    district_type,
    district_name,
    voters_in_block_district,
    voters_in_block,
    block_population,
    pct_of_geo,
    -- fractional by design: keeping it unrounded makes allocations sum exactly
    -- to block_population per (block, district_type) -- the mass-conservation
    -- invariant the consumers rely on.
    pct_of_geo * block_population as allocated_population,
    is_non_exact,
    loaded_at
from joined
