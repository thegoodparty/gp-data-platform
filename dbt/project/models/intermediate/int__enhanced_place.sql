{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_place"],
    )
}}

with

    enhanced_place as (
        select
            {{ generate_salted_uuid(fields=["id"], salt="ballotready") }} as id,
            created_at,
            updated_at,
            -- id as br_hash_id, # should be added to API data model
            database_id as br_database_id,
            `name`,
            slug,
            geo_id,
            mtfcc,
            `state`
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select id, created_at, updated_at, br_database_id, `name`, slug, geo_id, mtfcc, `state`
from enhanced_place
