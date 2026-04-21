-- All BallotReady elections must appear in the election mart.
-- BR is the authoritative spine; zero missing rows expected.
select br.gp_election_id
from {{ ref("int__civics_election_ballotready") }} as br
left join {{ ref("election") }} as e on br.gp_election_id = e.gp_election_id
where e.gp_election_id is null
