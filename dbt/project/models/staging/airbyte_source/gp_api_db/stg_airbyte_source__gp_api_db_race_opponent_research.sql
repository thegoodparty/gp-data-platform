with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_race_opponent_research") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            kind,
            run_id,
            status,
            attempts,
            campaign_id,
            opponent_name,
            election_candidacy_id,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            cast(completed_at as timestamp) as completed_at,
            cast(last_viewed_at as timestamp) as last_viewed_at
        from source
    )
select *
from renamed
