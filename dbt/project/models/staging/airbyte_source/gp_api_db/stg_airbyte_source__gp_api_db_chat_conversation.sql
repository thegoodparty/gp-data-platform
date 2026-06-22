with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_chat_conversation") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("scope") }},
            {{ adapter.quote("title") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("deleted_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("owner_user_id") }},
            {{ adapter.quote("organization_slug") }}

        from source
    )
select *
from renamed
