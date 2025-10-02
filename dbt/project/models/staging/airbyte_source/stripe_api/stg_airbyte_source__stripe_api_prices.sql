with
    source as (select * from {{ source("airbyte_source", "stripe_api_prices") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("active") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("product") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("nickname") }},
            {{ adapter.quote("recurring") }},
            {{ adapter.quote("is_deleted") }},
            {{ adapter.quote("lookup_key") }},
            {{ adapter.quote("tiers_mode") }},
            {{ adapter.quote("unit_amount") }},
            {{ adapter.quote("tax_behavior") }},
            {{ adapter.quote("billing_scheme") }},
            {{ adapter.quote("custom_unit_amount") }},
            {{ adapter.quote("transform_quantity") }},
            {{ adapter.quote("unit_amount_decimal") }}

        from source
    )
select *
from renamed
