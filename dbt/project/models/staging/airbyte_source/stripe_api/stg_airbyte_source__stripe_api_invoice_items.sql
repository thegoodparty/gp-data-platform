with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_invoice_items") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("plan") }},
            {{ adapter.quote("price") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("period") }},
            {{ adapter.quote("invoice") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("customer") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("quantity") }},
            {{ adapter.quote("discounts") }},
            {{ adapter.quote("proration") }},
            {{ adapter.quote("tax_rates") }},
            {{ adapter.quote("is_deleted") }},
            {{ adapter.quote("test_clock") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("unit_amount") }},
            {{ adapter.quote("discountable") }},
            {{ adapter.quote("subscription") }},
            {{ adapter.quote("subscription_item") }},
            {{ adapter.quote("unit_amount_decimal") }}

        from source
    )
select *
from renamed
