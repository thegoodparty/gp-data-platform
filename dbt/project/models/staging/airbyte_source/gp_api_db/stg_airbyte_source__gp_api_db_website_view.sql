with
    source as (select * from {{ source("airbyte_source", "gp_api_db_website_view") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("visitor_id") }},
            {{ adapter.quote("website_id") }}

        from source
    )
select *
from renamed
