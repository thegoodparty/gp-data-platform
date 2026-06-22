with
    source as (
        select * from {{ source("airbyte_source", "amplitude_taxonomy_event_type") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("event_type") }},
            {{ adapter.quote("display_name") }},
            {{ adapter.quote("description") }},
            {{ adapter.quote("is_active") }},
            -- category arrives as a JSON object string, e.g. {"name":"win_onboarding"};
            -- flatten to its name in the staging layer so consumers read a plain
            -- string.
            get_json_object({{ adapter.quote("category") }}, '$.name') as category_name,
            -- tags arrives as a JSON array string; parse to array<string> per the
            -- taxonomy API contract (empty/uncurated today, so usually null).
            from_json({{ adapter.quote("tags") }}, 'array<string>') as tags,
            {{ adapter.quote("owner") }},
            {{ adapter.quote("is_hidden_from_dropdowns") }},
            {{ adapter.quote("is_hidden_from_persona_results") }},
            {{ adapter.quote("is_hidden_from_pathfinder") }},
            {{ adapter.quote("is_hidden_from_timeline") }}
        from source
    )
select *
from renamed
