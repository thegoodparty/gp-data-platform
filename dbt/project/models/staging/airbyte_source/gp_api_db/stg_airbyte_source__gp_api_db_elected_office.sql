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
            is_active,
            created_at,
            updated_at,
            campaign_id,
            elected_date,
            sworn_in_date,
            term_end_date,
            term_start_date,
            term_length_days
        from source
    )
select *
from renamed
