with
    source as (select * from {{ source("airbyte_source", "gp_api_db_municipality") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("data") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("slug") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("county_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }}

        from source
    )
select *
from renamed
