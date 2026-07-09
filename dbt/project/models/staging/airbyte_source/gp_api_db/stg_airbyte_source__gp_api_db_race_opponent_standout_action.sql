with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_race_opponent_standout_action") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            body,
            issue,
            {{ adapter.quote("order") }},
            title,
            run_id,
            campaign_id,
            sms_message,
            opponent_name,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    )
select *
from renamed
