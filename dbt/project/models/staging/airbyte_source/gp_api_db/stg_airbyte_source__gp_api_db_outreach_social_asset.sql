with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_outreach_social_asset") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("outreach_social_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("platform") }},
            {{ adapter.quote("kind") }},
            {{ adapter.quote("text") }},
            {{ adapter.quote("caption") }}
        from source
    )
select *
from renamed
