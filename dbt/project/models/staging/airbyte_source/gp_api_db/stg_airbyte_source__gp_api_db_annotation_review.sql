with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_annotation_review") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            body,
            reviewer_email,
            reviewer_clerk_sub,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    )
select *
from renamed
