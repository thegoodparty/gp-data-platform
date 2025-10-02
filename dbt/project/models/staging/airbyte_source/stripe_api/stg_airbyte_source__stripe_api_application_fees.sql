with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_application_fees") }}
    ),
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
            {{ adapter.quote("source") }},
            {{ adapter.quote("account") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("refunds") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("refunded") }},
            {{ adapter.quote("application") }},
            {{ adapter.quote("amount_refunded") }},
            {{ adapter.quote("balance_transaction") }},
            {{ adapter.quote("originating_transaction") }}

        from source
    )
select *
from renamed
