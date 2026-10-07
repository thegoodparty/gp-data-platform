-- gp_api_people unions the two, so a shared id would make it non-unique. An id
-- taken from LALVOTERID collides only if the anti-join missed a voter.
select consumer_only.id
from {{ ref("gp_api_consumer_only_people") }} as consumer_only
inner join {{ ref("gp_api_voters") }} as voters on consumer_only.id = voters.id
