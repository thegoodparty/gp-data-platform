-- is_non_exact must match the actual split, derived from the per-group row
-- count (not a float comparison): a (block, district_type) with exactly one
-- district_name is exact (is_non_exact=false on its row); >1 name is non-exact
-- (is_non_exact=true on all its rows). Count-based, robust.
select block_geoid, district_type
from {{ ref("int__district_census_allocation") }}
group by block_geoid, district_type
having
    (count(*) = 1 and max(case when is_non_exact then 1 else 0 end) = 1)
    or (count(*) > 1 and min(case when is_non_exact then 1 else 0 end) = 0)
