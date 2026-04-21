-- All BallotReady election_stages must appear in the election_stage mart.
-- BR is the authoritative spine; zero missing rows expected.
select br.gp_election_stage_id
from {{ ref("int__civics_election_stage_ballotready") }} as br
left join
    {{ ref("election_stage") }} as es
    on br.gp_election_stage_id = es.gp_election_stage_id
where es.gp_election_stage_id is null
