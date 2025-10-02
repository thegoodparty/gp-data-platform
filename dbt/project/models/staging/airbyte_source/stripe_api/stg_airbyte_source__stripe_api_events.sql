with
    source as (select * from {{ source("airbyte_source", "stripe_api_events") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("data") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("request") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("api_version") }},
            {{ adapter.quote("pending_webhooks") }}

        from source
    )
select *
from renamed
