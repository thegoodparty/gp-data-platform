-- Coverage guard on the position-to-district link. A position bound to a
-- district with no L2 voters looks matched but zeroes every voter-backed
-- surface for that campaign, and the vendor rewrites district names between
-- snapshots often enough to do this in bulk. The sibling test next to this one
-- only catches drift the spelling resolution knows how to absorb; this catches
-- a rename in a shape it does not, which would otherwise drain coverage
-- silently.
--
-- 99.1% of district-bound positions sit on a populated district today. The
-- floor leaves room for the long tail of offices L2 has no district for at all
-- while still tripping on a bulk rename: the last one moved ~1.4% of positions.
select
    count(*) as positions_with_district,
    count(district.registered_voters) as positions_on_populated_district
from {{ ref("m_election_api__position") }} as position
join
    {{ ref("m_election_api__district") }} as district
    on position.district_id = district.id
having
    count(*) = 0 or count(district.registered_voters) * 1.0 / nullif(count(*), 0) < 0.98
