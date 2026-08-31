-- The candidacy image renders a person's photo onto *other* candidates'
-- profiles, so suppressing the person mart alone leaves a reported photo public.
-- Resolves the BR candidate through the API intermediate, not the S3 feed: the
-- feed cannot resolve ~2.4k of the image-carrying candidacies (it omits many
-- upcoming general-stage rosters), which would let a live photo pass unseen.
with
    br as (
        select distinct
            cast(database_id as bigint) as br_candidacy_id,
            cast(candidate_database_id as bigint) as br_candidate_id
        from {{ ref("int__ballotready_candidacy") }}
        where database_id is not null and candidate_database_id is not null
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
