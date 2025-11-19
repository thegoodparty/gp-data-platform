with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_website_contact") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("message") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("website_id") }},
            {{ adapter.quote("sms_consent") }}

        from source
    )
select *
from renamed
