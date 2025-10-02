with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_invoice_line_items") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("plan") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("price") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("period") }},
            {{ adapter.quote("invoice") }},
            {{ adapter.quote("margins") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("quantity") }},
            {{ adapter.quote("discounts") }},
            {{ adapter.quote("proration") }},
            {{ adapter.quote("tax_rates") }},
            {{ adapter.quote("invoice_id") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("tax_amounts") }},
            {{ adapter.quote("discountable") }},
            {{ adapter.quote("invoice_item") }},
            {{ adapter.quote("subscription") }},
            {{ adapter.quote("invoice_created") }},
            {{ adapter.quote("invoice_updated") }},
            {{ adapter.quote("discount_amounts") }},
            {{ adapter.quote("proration_details") }},
            {{ adapter.quote("subscription_item") }},
            {{ adapter.quote("amount_excluding_tax") }},
            {{ adapter.quote("unit_amount_excluding_tax") }}

        from source
    )
select *
from renamed
