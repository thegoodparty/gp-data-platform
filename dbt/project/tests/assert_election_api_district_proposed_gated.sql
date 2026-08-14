-- A Proposed_District row in the District dimension is bindable, so only an
-- adopted and checked congressional map may appear. Anything else reaching this
-- mart (an unverified state, a state seeded current or needs_boundary, or a
-- non-congressional proposed value such as MI's state senate or a WA/CO
-- annexation) means the gate in m_election_api__district stopped working.
select district.state, district.l2_district_type, district.l2_district_name
from {{ ref("m_election_api__district") }} as district
left join
    {{ ref("district_map_adoption") }} as adoption
    on adoption.state = district.state
    and adoption.district_type = 'US_Congressional_District'
    and adoption.adopted_source = 'proposed'
    and adoption.is_verified
where
    district.l2_district_type = 'Proposed_District'
    and (
        not upper(district.l2_district_name) like '%PROPOSED CONG DIST%'
        or adoption.state is null
    )
