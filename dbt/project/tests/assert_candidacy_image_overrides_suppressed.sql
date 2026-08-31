-- The candidacy image renders a person's photo onto *other* candidates'
-- profiles, so suppressing the person mart alone leaves a reported photo public.
-- Resolves the BR candidate via the S3 feed rather than the mart's own join key,
-- so a change to that key cannot silently pass this test.
with
    br as (
        select distinct
            cast(br_candidacy_id as bigint) as br_candidacy_id,
            cast(br_candidate_id as bigint) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null and br_candidate_id is not null
    ),

    published as (
        select cast(br_database_id as bigint) as br_candidacy_id, id, image
        from {{ ref("m_election_api__candidacy") }}
        where image is not null
    )

select published.id
from published
inner join br on published.br_candidacy_id = br.br_candidacy_id
inner join
    {{ ref("election_api_person_image_overrides") }} as ovr
    on br.br_candidate_id = ovr.br_person_id
where ovr.suppress_image
