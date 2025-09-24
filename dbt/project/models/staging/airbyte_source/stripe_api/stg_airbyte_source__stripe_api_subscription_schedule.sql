with
    source as (
        select * from {{ source("airbyte_source", "stripe_api_subscription_schedule") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("phases") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("customer") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("test_clock") }},
            {{ adapter.quote("application") }},
            {{ adapter.quote("canceled_at") }},
            {{ adapter.quote("released_at") }},
            {{ adapter.quote("completed_at") }},
            {{ adapter.quote("end_behavior") }},
            {{ adapter.quote("subscription") }},
            {{ adapter.quote("current_phase") }},
            {{ adapter.quote("default_settings") }},
            {{ adapter.quote("renewal_interval") }},
            {{ adapter.quote("released_subscription") }}

        from source
    )
select *
from renamed
