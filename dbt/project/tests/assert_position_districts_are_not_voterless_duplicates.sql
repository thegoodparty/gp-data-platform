-- No position may sit on a district row that is a voterless duplicate of a
-- populated one. L2 rewrites district names between vintages, which mints a
-- second district row per place; landing a position on the empty spelling
-- zeroes every voter-backed surface for that campaign while still looking like
-- a successful match. Exactly zero once the spelling resolution is applied, so
-- a non-zero result means it stopped reaching the position model.
with resolved_districts as ({{ l2_district_spelling_resolution() }})
select
    position.br_database_id,
    position.name as position_name,
    district.state,
    district.l2_district_type,
    district.l2_district_name
from {{ ref("m_election_api__position") }} as position
join
    {{ ref("m_election_api__district") }} as district
    on position.district_id = district.id
join
    resolved_districts as resolved
    on district.state = resolved.state
    and district.l2_district_type = resolved.l2_district_type
    and district.l2_district_name = resolved.l2_district_name
where district.registered_voters is null and resolved.district_id != district.id
