with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_transfer_reversals") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("transfer") }},
            {{ adapter.quote("source_refund") }},
            {{ adapter.quote("balance_transaction") }},
            {{ adapter.quote("destination_payment_refund") }}

        from source
    )
select *
from renamed
