with
    source as (select * from {{ source("airbyte_source", "stripe_api_transfers") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("reversed") }},
            {{ adapter.quote("automatic") }},
            {{ adapter.quote("recipient") }},
            {{ adapter.quote("reversals") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("destination") }},
            {{ adapter.quote("source_type") }},
            {{ adapter.quote("arrival_date") }},
            {{ adapter.quote("transfer_group") }},
            {{ adapter.quote("amount_reversed") }},
            {{ adapter.quote("source_transaction") }},
            {{ adapter.quote("balance_transaction") }},
            {{ adapter.quote("destination_payment") }},
            {{ adapter.quote("statement_descriptor") }},
            {{ adapter.quote("statement_description") }},
            {{ adapter.quote("failure_balance_transaction") }}

        from source
    )
select *
from renamed
