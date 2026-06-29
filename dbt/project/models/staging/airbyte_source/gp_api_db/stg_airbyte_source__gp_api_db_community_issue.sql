with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_community_issue") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("organization_slug") }},
            {{ adapter.quote("list") }},
            {{ adapter.quote("rank") }},
            {{ adapter.quote("category") }},
            {{ adapter.quote("priority") }},
            {{ adapter.quote("title") }},
            {{ adapter.quote("summary") }},
            {{ adapter.quote("detail") }},
            {{ adapter.quote("last_refreshed_run_id") }},
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            cast(archived_at as timestamp) as archived_at
        from source
    )
select *
from renamed
