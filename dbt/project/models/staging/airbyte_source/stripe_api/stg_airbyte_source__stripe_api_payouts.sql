with
    source as (select * from {{ source("airbyte_source", "stripe_api_payouts") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("method") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("automatic") }},
            {{ adapter.quote("recipient") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("destination") }},
            {{ adapter.quote("reversed_by") }},
            {{ adapter.quote("source_type") }},
            {{ adapter.quote("arrival_date") }},
            {{ adapter.quote("bank_account") }},
            {{ adapter.quote("failure_code") }},
            {{ adapter.quote("source_balance") }},
            {{ adapter.quote("transfer_group") }},
            {{ adapter.quote("amount_reversed") }},
            {{ adapter.quote("failure_message") }},
            {{ adapter.quote("original_payout") }},
            {{ adapter.quote("source_transaction") }},
            {{ adapter.quote("balance_transaction") }},
            {{ adapter.quote("statement_descriptor") }},
            {{ adapter.quote("reconciliation_status") }},
            {{ adapter.quote("statement_description") }},
            {{ adapter.quote("failure_balance_transaction") }}

        from source
    )
select *
from renamed
