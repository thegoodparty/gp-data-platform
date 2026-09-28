-- An office on a voterless district is unpickable with an empty voter file; scoped
-- to the district types L2 covers in every state, where zero is achievable.
select position.br_database_id, position.name, district.state, district.l2_district_name
from {{ ref("m_election_api__position") }} as position
join
    {{ ref("m_election_api__district") }} as district
    on district.id = position.district_id
where
    district.l2_district_type in (
        'State_House_District',
        'State_Senate_District',
        'State_Legislative_District',
        'US_Congressional_District'
    )
    and district.registered_voters is null
