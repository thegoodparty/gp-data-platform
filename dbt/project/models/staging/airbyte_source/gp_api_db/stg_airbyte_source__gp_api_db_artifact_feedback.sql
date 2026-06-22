with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_artifact_feedback") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            comment,
            feedback,
            artifact_id,
            briefing_id,
            artifact_type,
            organization_slug,
            cast(submitter_user_id as int) as submitter_user_id,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    )
select *
from renamed
