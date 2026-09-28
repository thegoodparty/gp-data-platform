with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_door_knocking_stop") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("lat") }},
            {{ adapter.quote("lng") }},
            {{ adapter.quote("seq") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("leg_meters") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("leg_seconds") }},
            {{ adapter.quote("display_address") }},
            {{ adapter.quote("door_knocking_turf_id") }}

        from source
    )
select *
from renamed
