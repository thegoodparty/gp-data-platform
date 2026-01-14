with
    source as (select * from {{ source("airbyte_source", "gp_api_db_poll_issues") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("title") }},
            {{ adapter.quote("details") }},
            {{ adapter.quote("poll_id") }},
            {{ adapter.quote("summary") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("response_count") }},
            {{ adapter.quote("representativeComments") }}

        from source
    )
select *
from renamed
