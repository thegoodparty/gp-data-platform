with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_experiment_run") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            run_id,
            experiment_type,
            stage,
            status,
            priority,
            data_quality,
            error,
            params,
            cost_usd,
            duration_seconds,
            resume_attempts,
            artifact_bucket,
            artifact_key,
            organization_slug,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            cast(resume_scheduled_for as timestamp) as resume_scheduled_for
        from source
    )
select *
from renamed
