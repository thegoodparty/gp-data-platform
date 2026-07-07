-- A BallotReady br_candidate_id must map to at most one gp_person_id. BR is the
-- person grain, so a split would mean the same person got two ids.
with
    br as (
        select substring_index(record_key, '|', -1) as br_candidate_id, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
        where source_name = 'ballotready'
    )

select br_candidate_id, count(distinct gp_person_id) as distinct_ids
from br
group by br_candidate_id
having count(distinct gp_person_id) > 1
