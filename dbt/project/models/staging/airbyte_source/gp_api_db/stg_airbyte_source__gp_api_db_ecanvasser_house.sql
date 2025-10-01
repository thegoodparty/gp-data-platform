with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_ecanvasser_house") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("address") }},
            {{ adapter.quote("latitude") }},
            {{ adapter.quote("longitude") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("ecanvasser_id") }}

        from source
    )
select *
from renamed
