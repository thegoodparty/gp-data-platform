with
    source as (select * from {{ source("airbyte_source", "hubspot_api_owners") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("teams") }},
            {{ adapter.quote("userId") }} as user_id,
            {{ adapter.quote("archived") }},
            {{ adapter.quote("lastName") }} as last_name,
            {{ adapter.quote("createdAt") }} as created_at,
            {{ adapter.quote("firstName") }} as first_name,
            {{ adapter.quote("updatedAt") }} as updated_at,
            {{ adapter.quote("userIdIncludingInactive") }} as user_id_including_inactive

        from source
    )
select *
from renamed
