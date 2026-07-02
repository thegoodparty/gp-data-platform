-- Every resolved statewide official must bind the 'State' stats row on the
-- FULL 3-tuple. Pins the postal-code district_name contract that the
-- documented district_census_stats per-official join recipe relies on (the
-- recipe works WITHOUT an is_statewide branch only while this holds).
select r.organization_slug
from {{ ref("int__serve_district_resolution") }} r
left join
    {{ ref("district_census_stats") }} s
    on r.state = s.state_postal_code
    and r.l2_district_type = s.district_type
    and r.normalized_district_name = s.district_name
where r.is_statewide and s.state_postal_code is null
