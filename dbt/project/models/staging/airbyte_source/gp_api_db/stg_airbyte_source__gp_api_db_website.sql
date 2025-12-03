with
    source as (select * from {{ source("airbyte_source", "gp_api_db_website") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("content") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("campaign_id") }},
            {{ adapter.quote("vanity_path") }}

        from source
    )
select *
from renamed
