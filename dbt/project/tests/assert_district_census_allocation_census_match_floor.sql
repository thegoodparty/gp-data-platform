{{ config(severity="warn") }}

-- Census-join coverage canary (DATA-1992). The substrate inner-joins the block
-- map to the 2020 census frame; blocks with L2 voters but no census-frame row
-- (geocoding artifacts, ~0.13% today) are dropped. Warn if the substrate keeps
-- <99% of the map's distinct blocks: more than the known artifact share would
-- signal census source/vintage drift. A band canary, not a fixed count.
with
    map_blocks as (
        select count(distinct block_geoid) as n
        from {{ ref("int__l2_block_district_map") }}
    ),
    matched as (
        select count(distinct block_geoid) as n
        from {{ ref("int__district_census_allocation") }}
    )
select map_blocks.n as map_blocks, matched.n as matched_blocks
from map_blocks, matched
having matched.n * 1.0 / nullif(map_blocks.n, 0) < 0.99
