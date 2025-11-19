with
    source as (select * from {{ source("airbyte_source", "gp_api_db_domain") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("price") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("payment_id") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("website_id") }},
            {{ adapter.quote("operation_id") }},
            {{ adapter.quote("email_forwarding_domain_id") }}

        from source
    )
select *
from renamed
