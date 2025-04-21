with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_campaign_position") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("order") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("campaign_id") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("position_id") }},
            {{ adapter.quote("top_issue_id") }}

        from source
    )
select *
from renamed
