with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_promotion_codes") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("code") }},
            {{ adapter.quote("active") }},
            {{ adapter.quote("coupon") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("customer") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("expires_at") }},
            {{ adapter.quote("restrictions") }},
            {{ adapter.quote("times_redeemed") }},
            {{ adapter.quote("max_redemptions") }}

        from source
    )
select *
from renamed
