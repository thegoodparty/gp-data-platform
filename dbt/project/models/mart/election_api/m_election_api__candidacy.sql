{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "candidacy"],
    )
}}


with
    latest_candidacy as (
        select
            {{ generate_salted_uuid(fields=["id"], salt="ballotready") }} as id,
            id as br_hash_id,
            database_id as br_database_id,
            candidate_database_id,
            race_database_id,
            created_at,
            updated_at
        from {{ ref("int__ballotready_candidacy") }}
        where
            1 = 1
            {% if is_incremental() %}
                and updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    ),
    tbl_party as (
        select
            candidacy_id,
            case when size(parties) > 0 then parties[0].name else null end as party
        from {{ ref("int__ballotready_party") }}
    ),
    enhanced_candidacy as (
        select
            tbl_candidacy.id,
            tbl_candidacy.br_database_id,
            tbl_candidacy.created_at,
            tbl_candidacy.updated_at,
            tbl_candidacy.race_database_id,
            tbl_party.party,
            tbl_person.first_name,
            tbl_person.last_name,
            case
                when size(tbl_person.images) > 0 then tbl_person.images[0].url else null
            end as image,
            tbl_person.bio_text as about,
            transform(tbl_person.urls, url -> url.url) as urls,
            tbl_place.state,
            tbl_place.name as place_name,
            tbl_int_race.position_name as position_name,
            tbl_mart_race.frequency as election_frequency,
            tbl_mart_race.salary,
            tbl_mart_race.normalized_position_name,
            tbl_mart_race.position_description,
            concat(
                coalesce(tbl_person.first_name, ''),
                '-',
                coalesce(tbl_person.last_name, '')
            ) as first_last_name_slug,
            tbl_mart_race.id as race_id
        from latest_candidacy as tbl_candidacy
        left join
            tbl_party as tbl_party
            on tbl_candidacy.br_database_id = tbl_party.candidacy_id
        left join
            {{ ref("int__ballotready_person") }} as tbl_person
            on tbl_candidacy.candidate_database_id = tbl_person.database_id
        left join
            {{ ref("m_election_api__race") }} as tbl_mart_race
            on tbl_candidacy.race_database_id = tbl_mart_race.br_database_id
        left join
            {{ ref("int__enhanced_race") }} as tbl_int_race
            on tbl_mart_race.br_database_id = tbl_int_race.br_database_id
        left join
            {{ ref("m_election_api__place") }} as tbl_place
            on tbl_mart_race.place_id = tbl_place.id
    ),
    non2party_with_person_and_slug as (
        select
            id,
            br_database_id,
            created_at,
            updated_at,
            party,
            state,
            place_name,
            race_database_id,
            first_name,
            last_name,
            image,
            about,
            urls,
            election_frequency,
            salary,
            normalized_position_name,
            position_name,
            position_description,
            first_last_name_slug,
            case
                when position_name is not null
                then
                    concat(
                        {{ slugify("first_last_name_slug") }},
                        '/',
                        {{ slugify("position_name") }}
                    )
                else {{ slugify("first_last_name_slug") }}
            end as slug,
            race_id
        from enhanced_candidacy
        where
            1 = 1
            and first_name is not null
            and last_name is not null
            and not (
                party in ('Conservative Party', 'Progressive', 'DCP')
                or party ilike '%democrat%'
                or party ilike '%republican%'
            )
    ),
    deduped_candidacy as (
        select *
        from non2party_with_person_and_slug
        qualify row_number() over (partition by slug order by updated_at desc) = 1
    )

select
    id,
    br_database_id,
    created_at,
    updated_at,
    party,
    state,
    place_name,
    race_database_id,
    first_name,
    last_name,
    image,
    about,
    urls,
    election_frequency,
    salary,
    normalized_position_name,
    position_name,
    position_description,
    slug,
    race_id
from deduped_candidacy
