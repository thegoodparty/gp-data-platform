with
    source as (select * from {{ source("airbyte_source", "gp_api_db_poll") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("image_url") }},
            {{ adapter.quote("confidence") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("is_completed") }},
            {{ adapter.quote("completed_date") }},
            {{ adapter.quote("response_count") }},
            {{ adapter.quote("scheduled_date") }},
            {{ adapter.quote("message_content") }},
            {{ adapter.quote("elected_office_id") }},
            {{ adapter.quote("targetAudienceSize") }},
            {{ adapter.quote("estimatedCompletionDate") }}

        from source
    )
select *
from renamed
