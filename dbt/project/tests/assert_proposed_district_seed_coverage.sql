-- Every (state, district type) carrying a proposed map in L2 must have an
-- explicit adoption decision. Absence of a seed row silently means "current",
-- which is the stale map for a jurisdiction that redistricted, so a state newly
-- appearing in the vendor feed has to fail loudly rather than quietly resolve to
-- the old map.
--
-- Reads the voter file rather than a district model so it stays independent of
-- how the map is consumed downstream: anything that filters unadopted values
-- would hide a brand-new unseeded state, which is the case this exists to catch.
--
-- Covers both handled types. The type comes from parsing the value, never from
-- the column name, because one column carries both.
with
    vendor_decisions_needed as (
        select distinct
            l2.state_postal_code as state,
            {{ proposed_district_type("l2.proposed_district") }} as district_type
        from {{ ref("int__l2_nationwide_uniform") }} as l2
        where {{ is_proposed_handled_district("l2.proposed_district") }}
    )
select needed.state, needed.district_type
from vendor_decisions_needed as needed
left join
    {{ ref("district_map_adoption") }} as adoption
    on adoption.state = needed.state
    and adoption.district_type = needed.district_type
where adoption.state is null
