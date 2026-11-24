-- Array columns land as arrays; Airbyte delivered them as JSON text.
with
    source as (select * from {{ source("amplitude_ingestion", "cohorts") }}),
    renamed as (
        select
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("size") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("appId") }},
            {{ adapter.quote("hidden") }},
            to_json(owners) as owners,
            {{ adapter.quote("edit_id") }},
            {{ adapter.quote("lastMod") }},
            to_json(viewers) as viewers,
            {{ adapter.quote("archived") }},
            {{ adapter.quote("chart_id") }},
            {{ adapter.quote("finished") }},
            to_json(metadata) as metadata,
            {{ adapter.quote("createdAt") }},
            {{ adapter.quote("published") }},
            {{ adapter.quote("definition") }},
            {{ adapter.quote("popularity") }},
            {{ adapter.quote("view_count") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("last_viewed") }},
            {{ adapter.quote("location_id") }},
            {{ adapter.quote("lastComputed") }},
            to_json(shortcut_ids) as shortcut_ids,
            {{ adapter.quote("is_predictive") }},
            {{ adapter.quote("is_official_content") }}

        from source
    )
select *
from renamed
