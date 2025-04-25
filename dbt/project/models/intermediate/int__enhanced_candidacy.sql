{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
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
            created_at,
            updated_at,
            result
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
            tbl_person.first_name,
            tbl_person.last_name,
            -- TODO: get party,
            -- TODO: get city and state (candidace -> race -> place)
            case
                when size(tbl_person.images) > 0 then tbl_person.images[0].url else null
            end as image,
            tbl_person.about,
            transform(tbl_person.urls, url -> url.url) as urls
        -- TODO: join to gp-api data
        -- TODO: add position/place data (candidacy -> race -> position)
        from latest_candidacy as tbl_candidacy
        left join
            {{ ref("int__enhanced_person") }} as tbl_person
            on tbl_candidacy.candidate_database_id = tbl_person.database_id
    )

select
    id,
    br_database_id,
    created_at,
    updated_at,
    first_name,
    last_name,
    image,
    about,
    urls
from enhanced_columns
