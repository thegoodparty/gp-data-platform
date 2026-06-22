with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_meeting_briefing") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            artifact,
            artifact_key,
            artifact_bucket,
            meeting_time,
            meeting_timezone,
            elected_office_id,
            experiment_run_id,
            cast(meeting_date as date) as meeting_date,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    )
select *
from renamed
