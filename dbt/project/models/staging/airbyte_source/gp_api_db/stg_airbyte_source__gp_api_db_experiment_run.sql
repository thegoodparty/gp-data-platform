with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_experiment_run") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("run_id") }},
            {{ adapter.quote("experiment_type") }},
            {{ adapter.quote("stage") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("priority") }},
            {{ adapter.quote("data_quality") }},
            {{ adapter.quote("error") }},
            {{ adapter.quote("params") }},
            {{ adapter.quote("cost_usd") }},
            {{ adapter.quote("duration_seconds") }},
            {{ adapter.quote("resume_attempts") }},
            {{ adapter.quote("artifact_bucket") }},
            {{ adapter.quote("artifact_key") }},
            {{ adapter.quote("organization_slug") }},
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            cast(resume_scheduled_for as timestamp) as resume_scheduled_for
        from source
    )
select *
from renamed
