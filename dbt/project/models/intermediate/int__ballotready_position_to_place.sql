{{
    config(
        materialized="view",
        tags=["intermediate", "ballotready", "position_place_mapping"],
        schema="intermediate",
    )
}}

with
    exploded_table as (
        select
            database_id as position_database_id,
            explode(places.nodes) as places,
            created_at as position_created_at,
            updated_at as position_updated_at
        from {{ ref("stg_airbyte_source__ballotready_api_position_to_place") }}
    ),
    deduped_table as (
        select
            position_database_id,
            position_created_at,
            position_updated_at,
            places.databaseid as place_database_id
        from exploded_table
        group by
            position_database_id,
            position_created_at,
            position_updated_at,
            place_database_id
    )

select position_database_id, position_created_at, position_updated_at, place_database_id
from deduped_table
