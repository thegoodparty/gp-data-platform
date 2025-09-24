with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_subscription_items") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("plan") }},
            {{ adapter.quote("price") }},
            {{ adapter.quote("start") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("customer") }},
            {{ adapter.quote("discount") }},
            {{ adapter.quote("ended_at") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("quantity") }},
            {{ adapter.quote("tax_rates") }},
            {{ adapter.quote("trial_end") }},
            {{ adapter.quote("canceled_at") }},
            {{ adapter.quote("tax_percent") }},
            {{ adapter.quote("trial_start") }},
            {{ adapter.quote("subscription") }},
            {{ adapter.quote("billing_thresholds") }},
            {{ adapter.quote("current_period_end") }},
            {{ adapter.quote("cancel_at_period_end") }},
            {{ adapter.quote("current_period_start") }},
            {{ adapter.quote("subscription_updated") }},
            {{ adapter.quote("application_fee_percent") }}

        from source
    )
select *
from renamed
