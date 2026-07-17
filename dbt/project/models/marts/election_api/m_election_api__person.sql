{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}

-- election-api Person spine (one row per canonical gp_person_id). Full-coverage:
-- every person referenced by a synced Candidacy or OfficeHolder gets a row so
-- both child FKs resolve. The civics `people` mart is the coverage backbone
-- (it has a row for every gp_person_id, incl. officeholder-only and GP-native
-- people); int__ballotready_person supplies the BallotReady-granularity profile
-- fields (bio, headshot, typed social links, degrees, experiences, name parts).
with
    -- Person ids the children will reference. Union guarantees a Person row for
    -- every FK target; candidacy/office_holder do not read this mart, so no cycle.
    referenced_persons as (
        select gp_candidate_id as gp_person_id
        from {{ ref("m_election_api__candidacy") }}
        where gp_candidate_id is not null
        union
        select person_id as gp_person_id
        from {{ ref("m_election_api__office_holder") }}
        where person_id is not null
    ),

    -- BallotReady person profile, resolved to the canonical id via the person
    -- crosswalk (record_key 'ballotready|<database_id>'). Deduped to one
    -- representative BR person per gp_person_id (a merged cluster can carry >1).
    person_xwalk as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
        where record_key like 'ballotready|%'
    ),

    br_person as (
        select
            xw.gp_person_id,
            brp.database_id as br_person_id,
            brp.first_name,
            brp.middle_name,
            brp.last_name,
            brp.nickname,
            brp.suffix,
            brp.full_name,
            brp.bio_text,
            get(filter(brp.images, x -> x.url is not null), 0).url as headshot_url,
            get(
                filter(brp.urls, x -> x.type = 'website' and x.url is not null), 0
            ).url as website_url,
            get(
                filter(brp.urls, x -> x.type = 'linkedin' and x.url is not null), 0
            ).url as linkedin_url,
            get(
                filter(brp.urls, x -> x.type = 'facebook' and x.url is not null), 0
            ).url as facebook_url,
            get(
                filter(brp.urls, x -> x.type = 'twitter' and x.url is not null), 0
            ).url as twitter_url,
            get(
                filter(brp.urls, x -> x.type = 'instagram' and x.url is not null), 0
            ).url as instagram_url,
            to_json(brp.degrees) as degrees,
            to_json(brp.experiences) as experiences,
            brp.created_at,
            brp.updated_at
        from {{ ref("int__ballotready_person") }} as brp
        inner join
            person_xwalk as xw
            on xw.record_key = 'ballotready|' || cast(brp.database_id as string)
        qualify
            row_number() over (
                partition by xw.gp_person_id
                order by brp.updated_at desc nulls last, brp.database_id desc
            )
            = 1
    ),

    -- Canonical hub: guarantees coverage + supplies email/phone/state.
    people as (
        select
            gp_person_id,
            br_person_id,
            first_name,
            last_name,
            email,
            phone,
            state,
            first_seen_at
        from {{ ref("people") }}
    )

select
    rp.gp_person_id as id,
    coalesce(brp.created_at, ppl.first_seen_at, current_timestamp()) as created_at,
    coalesce(brp.updated_at, current_timestamp()) as updated_at,
    coalesce(brp.br_person_id, cast(ppl.br_person_id as int)) as br_person_id,

    -- Name parts: BallotReady-first, fall back to the canonical hub.
    coalesce(brp.first_name, ppl.first_name) as first_name,
    brp.middle_name,
    coalesce(brp.last_name, ppl.last_name) as last_name,
    brp.nickname,
    brp.suffix,
    coalesce(
        brp.full_name,
        nullif(
            trim(
                concat_ws(
                    ' ',
                    coalesce(brp.first_name, ppl.first_name),
                    coalesce(brp.last_name, ppl.last_name)
                )
            ),
            ''
        )
    ) as full_name,

    -- slugify(first-last); cosmetic, non-unique (the URL disambiguates on id).
    {{
        slugify(
            "concat_ws('-', coalesce(brp.first_name, ppl.first_name), coalesce(brp.last_name, ppl.last_name))"
        )
    }}
    as slug,

    brp.bio_text,
    brp.headshot_url,
    brp.website_url,
    brp.linkedin_url,
    brp.facebook_url,
    brp.twitter_url,
    brp.instagram_url,

    ppl.email,
    ppl.phone,

    brp.degrees,
    brp.experiences,

    ppl.state
from referenced_persons as rp
left join people as ppl on rp.gp_person_id = ppl.gp_person_id
left join br_person as brp on rp.gp_person_id = brp.gp_person_id
