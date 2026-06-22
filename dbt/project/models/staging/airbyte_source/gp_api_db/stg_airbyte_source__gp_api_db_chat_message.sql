with
    source as (select * from {{ source("airbyte_source", "gp_api_db_chat_message") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("role") }},
            {{ adapter.quote("content") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("conversation_id") }},
            {{ adapter.quote("client_message_id") }}

        from source
    )
select *
from renamed
