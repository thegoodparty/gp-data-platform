{{
    config(
        materialized="view",
        incremental_strategy="merge",
        unique_key="geo_id",
        on_schema_change="fail",
        tags=["intermediate", "ballotready", "geo_id_attributes"],
    )
}}

with
    unique_geo_ids as (
        select distinct geo_id
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        {% if is_incremental() %}
            where geo_id not in (select id from {{ this }})
        {% endif %}
        limit 100
    ),

    split_indices as (
        select
            geo_id,
            case
                when length(geo_id) = 2
                then array[]::int[]
                when length(geo_id) = 4
                then array[2]
                when length(geo_id) = 5
                then array[2]
                when length(geo_id) = 7
                then array[2]
                when length(geo_id) = 10
                then array[2, 5]
                when length(geo_id) = 11
                then array[2, 5]
                when length(geo_id) = 12
                then array[2, 5, 11]
            end as split_indices,
        from unique_geo_ids
    )

select *
from split_indices
