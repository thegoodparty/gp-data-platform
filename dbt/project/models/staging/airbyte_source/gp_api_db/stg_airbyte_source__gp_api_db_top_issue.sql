with
    source as (select * from {{ source("airbyte_source", "gp_api_db_top_issue") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }}

        from source
    )
select *
from renamed
