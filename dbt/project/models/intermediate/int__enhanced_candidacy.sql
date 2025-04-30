{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_candidacy"],
    )
}}

with
    -- TODO: maybe restrict to only the races in the election mart
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
            -- TODO: get city and state (candidacy -> race mart -> place mart -> values)
            tbl_race.frequency as election_frequency,
            tbl_race.salary,
            tbl_race.normalized_position_name,
            tbl_race.position_description
        /* TODO: add the following
                electionFrequency      String? // Formerly called term
                salary                 Int?
                normalizedPositionName String?
                positionDescription    String?
            */
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
    -- left join candidacy_to_race as c2r
    -- on tbl_candidacy.br_database_id = c2r.candidacy_database_id
    )

-- select * from candidacy_to_race
select
    id,
    br_database_id,
    created_at,
    updated_at,
    party,
    race_database_id,
    first_name,
    last_name,
    image,
    bio_text,
    urls,
    election_frequency,
    salary,
    normalized_position_name,
    position_description
from enhanced_columns
