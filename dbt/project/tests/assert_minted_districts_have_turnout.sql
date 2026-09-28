-- Every minted district must carry a projected-turnout row.
--
-- A proposed district has no projection of its own, so the mart carries the
-- same-numbered current district's rows across. That carry-over is the fallback,
-- and this is the only thing that checks it landed: win number and voter-contact
-- goal derive from turnout alone, so a district that ends up with none collapses
-- both to the -1 sentinel and takes a campaign's targets away.
--
-- Fails on absence, like the mint guard, because that is the shape of this
-- failure. Nothing is wrong in the data to find - a row is simply missing, and no
-- relationship or not_null test can see that. Two ways it happens: the turnout
-- model drops coverage for the current district the carry-over reads from, or the
-- two are built out of order, since both hash the same salted district_id.
with
    districts_with_turnout as (
        select distinct district_id from {{ ref("m_election_api__projected_turnout") }}
    )

select minted.state_postal_code, minted.district_type, minted.district_name
from {{ ref("int__l2_proposed_district_aggregations") }} as minted
left join districts_with_turnout on districts_with_turnout.district_id = minted.id
where districts_with_turnout.district_id is null
