-- Every state carrying a proposed congressional map in L2 must have an explicit
-- adoption decision. Absence of a seed row silently means "current", which is
-- the stale map for a state that redistricted, so a new state appearing in the
-- vendor feed has to fail loudly rather than quietly resolve to the old map.
--
-- Reads the voter file rather than the district aggregations because those now
-- drop unadopted proposed values upstream: a brand-new unseeded state would be
-- filtered out before this test could see it, which is precisely the case it
-- exists to catch. One column and a distinct is cheap next to that.
select distinct l2.state_postal_code
from {{ ref("int__l2_nationwide_uniform") }} as l2
left join
    {{ ref("district_map_adoption") }} as adoption
    on adoption.state = l2.state_postal_code
    and adoption.district_type = 'US_Congressional_District'
where {{ is_proposed_cong_dist("l2.proposed_district") }} and adoption.state is null
