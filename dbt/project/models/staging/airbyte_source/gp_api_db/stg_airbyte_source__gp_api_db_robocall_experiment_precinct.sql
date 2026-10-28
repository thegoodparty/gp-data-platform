with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_robocall_experiment_precinct") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("experiment_key") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("county") }},
            {{ adapter.quote("precinct") }},
            {{ adapter.quote("arm") }},
            -- a float in gp-api; Airbyte lands it as decimal
            cast(p_treatment as double) as p_treatment,
            {{ adapter.quote("assigned_for_district_id") }},
            {{ adapter.quote("assigned_for_campaign_id") }},
            {{ adapter.quote("landlines_at_assignment") }},
            {{ adapter.quote("created_at") }}
        from source
    )
select *
from renamed
