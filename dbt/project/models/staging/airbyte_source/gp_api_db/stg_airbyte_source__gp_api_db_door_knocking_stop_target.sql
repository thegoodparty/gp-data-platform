with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_door_knocking_stop_target") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("person_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("address_key") }},
            {{ adapter.quote("door_knocking_stop_id") }}

        from source
    )
select *
from renamed
