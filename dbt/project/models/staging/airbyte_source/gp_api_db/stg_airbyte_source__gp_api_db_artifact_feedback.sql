with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_artifact_feedback") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("comment") }},
            {{ adapter.quote("feedback") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("artifact_id") }},
            {{ adapter.quote("briefing_id") }},
            {{ adapter.quote("artifact_type") }},
            {{ adapter.quote("organization_slug") }},
            {{ adapter.quote("submitter_user_id") }}

        from source
    )
select *
from renamed
