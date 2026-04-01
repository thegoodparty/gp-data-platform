with
    source as (select * from {{ source("airbyte_source", "gp_api_db_organization") }}),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            slug,
            owner_id,
            position_id,
            custom_position_name,
            override_district_id,
            created_at,
            updated_at
        from source
    )
select *
from renamed
