with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_campaign_story") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            campaign_id,
            background,
            rewrite_count,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    )
select *
from renamed
