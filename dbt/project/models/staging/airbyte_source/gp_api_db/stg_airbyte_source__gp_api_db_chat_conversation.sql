with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_chat_conversation") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            scope,
            title,
            anchor,
            organization_slug,
            cast(owner_user_id as int) as owner_user_id,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            cast(deleted_at as timestamp) as deleted_at
        from source
    )
select *
from renamed
