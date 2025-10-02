with
    source as (
        select *
        from {{ source("airbyte_source", "stripe_api_application_fees_refunds") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("fee") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("balance_transaction") }}

        from source
    )
select *
from renamed
