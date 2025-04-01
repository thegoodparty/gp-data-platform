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
            {{ adapter.quote("userId") }},
            {{ adapter.quote("archived") }},
            {{ adapter.quote("lastName") }},
            {{ adapter.quote("createdAt") }},
            {{ adapter.quote("firstName") }},
            {{ adapter.quote("updatedAt") }},
            {{ adapter.quote("userIdIncludingInactive") }}

        from source
    )
select *
from renamed
