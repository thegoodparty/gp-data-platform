with
    source as (select * from {{ source("airbyte_source", "gp_api_db_chat_message") }}),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            role,
            content,
            conversation_id,
            client_message_id,
            cast(created_at as timestamp) as created_at
        from source
    )
select *
from renamed
