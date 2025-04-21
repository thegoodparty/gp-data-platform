/*
Though each Race object has a single Position, the location of a place can have multiple positions.
*/
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="race_database_id",
        tags=["intermediate", "ballotready", "ballotready_race_to_positions"],
    )
}}

with
    race_pos_geo_id as (
        select
            tbl_race.database_id as race_database_id,
            tbl_race.updated_at as race_updated_at,
            tbl_position.geo_id as position_geo_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_race
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
            on tbl_race.position.databaseid = tbl_position.database_id
        {% if is_incremental() %}
            where tbl_race.updated_at > (select max(race_updated_at) from {{ this }})
        {% endif %}
    ),
    race_all_pos_geo_ids as (
        select
            race_database_id,
            race_updated_at,
            tbl_position.database_id as position_database_id,
            tbl_position.geo_id as position_geo_id,
            tbl_position.name as position_name
        from race_pos_geo_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
            on race_pos_geo_id.position_geo_id = tbl_position.geo_id
        -- use normalized_position_datbase_id instead
        where tbl_position.geo_id is not null
    ),
    aggregated_positions as (
        select
            race_database_id,
            race_updated_at,
            array_agg(distinct position_name) as position_names
        from race_all_pos_geo_ids
        group by race_database_id, race_updated_at
    )
select distinct race_database_id, race_updated_at, position_names
from aggregated_positions
