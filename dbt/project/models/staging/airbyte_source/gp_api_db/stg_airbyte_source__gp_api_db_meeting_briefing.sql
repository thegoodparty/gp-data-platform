with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_meeting_briefing") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("artifact") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("artifact_key") }},
            {{ adapter.quote("meeting_date") }},
            {{ adapter.quote("meeting_time") }},
            {{ adapter.quote("artifact_bucket") }},
            {{ adapter.quote("meeting_timezone") }},
            {{ adapter.quote("elected_office_id") }},
            {{ adapter.quote("experiment_run_id") }}

        from source
    )
select *
from renamed
