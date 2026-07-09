-- Falsifiable person-identity contract (deferred from PR4): all candidacy_stage
-- rows sharing a BallotReady candidate (br_candidate_id, resolved from the
-- row's br_candidacy_id) must resolve to a single gp_person_id. BR person
-- identity is clean, so a violation signals a bad person edge or a min()
-- collision leaking across BR people.
with
    br as (
        select distinct
            cast(br_candidacy_id as string) as br_candidacy_id,
            cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null and br_candidate_id is not null
    ),

    rows as (
        select distinct br.br_candidate_id, cs.gp_person_id
        from {{ ref("candidacy_stage") }} as cs
        inner join br using (br_candidacy_id)
        where cs.gp_person_id is not null
    )

select br_candidate_id
from rows
group by br_candidate_id
having count(distinct gp_person_id) > 1
