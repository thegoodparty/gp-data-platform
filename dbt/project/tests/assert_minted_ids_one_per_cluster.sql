-- Stability property: every cluster resolves to exactly one minted id, so all
-- co-members share the same re-minted gp_candidacy_id / gp_election_stage_id.
select cluster_id, count(distinct minted_gp_candidacy_id) as n_ids
from {{ ref("int__civics_minted_candidacy_ids") }}
group by cluster_id
having count(distinct minted_gp_candidacy_id) > 1

union all

select cluster_id, count(distinct minted_gp_election_stage_id)
from {{ ref("int__civics_minted_election_stage_ids") }}
group by cluster_id
having count(distinct minted_gp_election_stage_id) > 1
