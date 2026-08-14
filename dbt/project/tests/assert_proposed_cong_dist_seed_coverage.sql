-- Every state carrying a proposed congressional map in L2 must have an explicit
-- adoption decision. Absence of a seed row silently means "current", which is
-- the stale map for a state that redistricted, so a new state appearing in the
-- vendor feed has to fail loudly rather than quietly resolve to the old map.
-- Read from the aggregations (one row per district) rather than voter grain.
select agg.state_postal_code
from {{ ref("int__l2_district_aggregations") }} as agg
left join
    {{ ref("district_map_adoption") }} as adoption
    on adoption.state = agg.state_postal_code
    and adoption.district_type = 'US_Congressional_District'
where
    agg.district_type = 'Proposed_District'
    and upper(agg.district_name) like '%PROPOSED CONG DIST%'
    and adoption.state is null
group by agg.state_postal_code
