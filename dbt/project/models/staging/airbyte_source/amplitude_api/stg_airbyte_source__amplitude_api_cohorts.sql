with
    source as (select * from {{ source("airbyte_source", "amplitude_api_cohorts") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("size") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("appId") }},
            {{ adapter.quote("hidden") }},
            {{ adapter.quote("owners") }},
            {{ adapter.quote("edit_id") }},
            {{ adapter.quote("lastMod") }},
            {{ adapter.quote("viewers") }},
            {{ adapter.quote("archived") }},
            {{ adapter.quote("chart_id") }},
            {{ adapter.quote("finished") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("createdAt") }},
            {{ adapter.quote("published") }},
            {{ adapter.quote("definition") }},
            {{ adapter.quote("popularity") }},
            {{ adapter.quote("view_count") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("last_viewed") }},
            {{ adapter.quote("location_id") }},
            {{ adapter.quote("lastComputed") }},
            {{ adapter.quote("shortcut_ids") }},
            {{ adapter.quote("is_predictive") }},
            {{ adapter.quote("is_official_content") }}

        from source
    )
select *
from renamed
