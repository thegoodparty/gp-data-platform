-- No staff or test account may reach the public /people profile feed. Internal
-- users file real (non-demo) campaigns while testing, so nothing upstream of the
-- profile scope keeps them out on its own.
select p.id, p.slug
from {{ ref("m_election_api__person") }} as p
inner join
    {{ ref("int__civics_internal_persons") }} as internal
    on internal.gp_person_id = p.id
