with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_elected_office") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            user_id,
            created_at,
            updated_at,
            campaign_id,
            sworn_in_date,
            organization_slug
        from source
    )
select *
from renamed
