with
    source as (select * from {{ source("airbyte_source", "stripe_api_transactions") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("card") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("dispute") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("cardholder") }},
            {{ adapter.quote("authorization") }},
            {{ adapter.quote("merchant_data") }},
            {{ adapter.quote("amount_details") }},
            {{ adapter.quote("merchant_amount") }},
            {{ adapter.quote("purchase_details") }},
            {{ adapter.quote("merchant_currency") }},
            {{ adapter.quote("balance_transaction") }}

        from source
    )
select *
from renamed
