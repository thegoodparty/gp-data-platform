with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_balance_transactions") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("fee") }},
            {{ adapter.quote("net") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("source") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("fee_details") }},
            {{ adapter.quote("available_on") }},
            {{ adapter.quote("exchange_rate") }},
            {{ adapter.quote("sourced_transfers") }},
            {{ adapter.quote("reporting_category") }}

        from source
    )
select *
from renamed
