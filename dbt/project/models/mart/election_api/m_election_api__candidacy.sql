{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["intermediate", "ballotready", "enhanced_candidacy"],
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
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    enhanced_columns as (
        select
            tbl_candidacy.id,
            tbl_candidacy.br_database_id,
            tbl_candidacy.created_at,
            tbl_candidacy.updated_at,
            tbl_candidacy.race_database_id,
            case
                when size(tbl_party.parties) > 0
                then tbl_party.parties[0].name
                else null
            end as party,
            tbl_person.first_name,
            tbl_person.last_name,
            case
                when size(tbl_person.images) > 0 then tbl_person.images[0].url else null
            end as image,
            tbl_person.bio_text,
            transform(tbl_person.urls, url -> url.url) as urls,
            tbl_place.state,
            tbl_place.name as place_name,
            tbl_race.frequency as election_frequency,
            tbl_race.salary,
            tbl_race.normalized_position_name,
            tbl_race.position_description,
            {{
                slugify(
                    "concat(tbl_person.first_name, '-', tbl_person.last_name, '-', tbl_race.normalized_position_name)"
                )
            }}
            as slug
        from latest_candidacy as tbl_candidacy
        left join
            {{ ref("int__ballotready_person") }} as tbl_person
            on tbl_candidacy.candidate_database_id = tbl_person.database_id
        left join
            {{ ref("int__ballotready_party") }} as tbl_party
            on tbl_candidacy.br_database_id = tbl_party.candidacy_id
        left join
            {{ ref("m_election_api__race") }} as tbl_race
            on tbl_candidacy.race_database_id = tbl_race.br_database_id
        left join
            {{ ref("m_election_api__place") }} as tbl_place
            on tbl_race.place_id = tbl_place.id
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
    bio_text,
    urls,
    election_frequency,
    salary,
    normalized_position_name,
    position_description,
    slug
from enhanced_columns
