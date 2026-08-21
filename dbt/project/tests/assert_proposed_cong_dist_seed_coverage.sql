-- Every state carrying a proposed congressional map in L2 must have an explicit
-- adoption decision. Absence of a seed row silently means "current", which is
-- the stale map for a state that redistricted, so a new state appearing in the
-- vendor feed has to fail loudly rather than quietly resolve to the old map.
--
-- Reads the voter file rather than a district model so it stays independent of
-- how the map is consumed downstream: anything that filters unadopted values
-- would hide a brand-new unseeded state, which is the case this exists to catch.
--
-- Congressional only, deliberately and temporarily. MI carries a proposed state
-- senate map that no seed row covers, so widening this now would fail the build
-- on a decision nobody has made yet. The seed already accepts a state senate
-- row; extending this guard is the last step of that research, not a precursor
-- to it. Until then the silent-default risk is real for MI and untested.
select distinct l2.state_postal_code
from {{ ref("int__l2_nationwide_uniform") }} as l2
left join
    {{ ref("district_map_adoption") }} as adoption
    on adoption.state = l2.state_postal_code
    and adoption.district_type = 'US_Congressional_District'
where {{ is_proposed_cong_dist("l2.proposed_district") }} and adoption.state is null
