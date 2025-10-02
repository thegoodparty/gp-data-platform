with
    source as (select * from {{ source("airbyte_source", "stripe_api_customers") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("cards") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("address") }},
            {{ adapter.quote("balance") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("sources") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("discount") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("shipping") }},
            {{ adapter.quote("tax_info") }},
            {{ adapter.quote("delinquent") }},
            {{ adapter.quote("is_deleted") }},
            {{ adapter.quote("tax_exempt") }},
            {{ adapter.quote("test_clock") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("default_card") }},
            {{ adapter.quote("subscriptions") }},
            {{ adapter.quote("default_source") }},
            {{ adapter.quote("invoice_prefix") }},
            {{ adapter.quote("account_balance") }},
            {{ adapter.quote("invoice_settings") }},
            {{ adapter.quote("preferred_locales") }},
            {{ adapter.quote("next_invoice_sequence") }},
            {{ adapter.quote("tax_info_verification") }}

        from source
    )
select *
from renamed
