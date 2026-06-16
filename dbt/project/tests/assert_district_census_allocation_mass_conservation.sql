-- Mass conservation (spec invariant, DATA-1992): within each represented
-- (block, district_type), the voter-share split sums to 1 and the allocated
-- population sums to the block's census population. A structural, spec-derived
-- test (no drifting count). Scope: this proves conservation for every group
-- that survived the L2 unpivot + census inner join; global completeness (the
-- dropped-block share) is covered by assert_..._census_match_floor.
select
    block_geoid,
    district_type,
    sum(pct_of_geo) as total_share,
    sum(allocated_population) as total_allocated,
    max(block_population) as block_population
from {{ ref("int__district_census_allocation") }}
group by block_geoid, district_type
having
    abs(sum(pct_of_geo) - 1.0) > 1e-6
    or abs(sum(allocated_population) - max(block_population)) > 1e-3
