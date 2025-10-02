with
    source as (select * from {{ source("airbyte_source", "stripe_api_products") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("url") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("active") }},
            {{ adapter.quote("images") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("caption") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("features") }},
            {{ adapter.quote("livemode") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("tax_code") }},
            {{ adapter.quote("shippable") }},
            {{ adapter.quote("attributes") }},
            {{ adapter.quote("is_deleted") }},
            {{ adapter.quote("unit_label") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("deactivate_on") }},
            {{ adapter.quote("default_price") }},
            {{ adapter.quote("package_dimensions") }},
            {{ adapter.quote("statement_descriptor") }}

        from source
    )
select *
from renamed
