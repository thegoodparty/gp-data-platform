with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_ecanvasser_interaction") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("notes") }},
            {{ adapter.quote("rating") }},
            {{ adapter.quote("source") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("contact_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("created_by") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("ecanvasser_id") }}

        from source
    )
select *
from renamed
