-- Every uploaded row mints a new HubSpot contact, so a candidacy whose person group
-- already holds one creates a second contact for a person we already have. The
-- email, phone, and BallotReady-id legs miss these: the same person reaches us from
-- a vendor under a different email, a different phone, and no BallotReady id.
select distinct f.gp_candidacy_id, f.gp_candidate_id
from {{ ref("candidacy_hubspot") }} as f
inner join
    {{ ref("int__civics_person_canonical_ids") }} as p
    on f.gp_candidate_id = p.gp_person_id
where p.source_name = 'hubspot'
