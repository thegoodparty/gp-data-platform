with
    source as (select * from {{ source("airbyte_source", "stripe_api_accounts") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("company") }},
            {{ adapter.quote("country") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("settings") }},
            {{ adapter.quote("controller") }},
            {{ adapter.quote("individual") }},
            {{ adapter.quote("capabilities") }},
            {{ adapter.quote("requirements") }},
            {{ adapter.quote("business_type") }},
            {{ adapter.quote("tos_acceptance") }},
            {{ adapter.quote("charges_enabled") }},
            {{ adapter.quote("payouts_enabled") }},
            {{ adapter.quote("business_profile") }},
            {{ adapter.quote("default_currency") }},
            {{ adapter.quote("details_submitted") }},
            {{ adapter.quote("external_accounts") }},
            {{ adapter.quote("future_requirements") }}

        from source
    )
select *
from renamed
