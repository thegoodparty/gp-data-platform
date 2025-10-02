with
    source as (select * from {{ source("airbyte_source", "stripe_api_refunds") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("charge") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("reason") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("payment_intent") }},
            {{ adapter.quote("receipt_number") }},
            {{ adapter.quote("transfer_reversal") }},
            {{ adapter.quote("balance_transaction") }},
            {{ adapter.quote("destination_details") }},
            {{ adapter.quote("source_transfer_reversal") }}

        from source
    )
select *
from renamed
