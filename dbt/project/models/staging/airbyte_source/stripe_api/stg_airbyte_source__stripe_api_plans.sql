with
    source as (select * from {{ source("airbyte_source", "stripe_api_plans") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("tiers") }},
            {{ adapter.quote("active") }},
            {{ adapter.quote("amount") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("product") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("currency") }},
            {{ adapter.quote("interval") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("nickname") }},
            {{ adapter.quote("is_deleted") }},
            {{ adapter.quote("tiers_mode") }},
            {{ adapter.quote("usage_type") }},
            {{ adapter.quote("amount_decimal") }},
            {{ adapter.quote("billing_scheme") }},
            {{ adapter.quote("interval_count") }},
            {{ adapter.quote("aggregate_usage") }},
            {{ adapter.quote("transform_usage") }},
            {{ adapter.quote("trial_period_days") }},
            {{ adapter.quote("statement_descriptor") }},
            {{ adapter.quote("statement_description") }}

        from source
    )
select *
from renamed
