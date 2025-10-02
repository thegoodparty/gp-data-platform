with
    source as (select * from {{ source("airbyte_source", "gp_api_db_outreach") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("error") }},
            {{ adapter.quote("title") }},
            {{ adapter.quote("script") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("message") }},
            {{ adapter.quote("createdAt") }},
            {{ adapter.quote("did_state") }},
            {{ adapter.quote("image_url") }},
            {{ adapter.quote("updatedAt") }},
            {{ adapter.quote("campaignId") }},
            {{ adapter.quote("project_id") }},
            {{ adapter.quote("identity_id") }},
            {{ adapter.quote("outreach_type") }},
            {{ adapter.quote("phone_list_id") }},
            {{ adapter.quote("audience_request") }},
            {{ adapter.quote("voter_file_filter_id") }}

        from source
    )
select *
from renamed
