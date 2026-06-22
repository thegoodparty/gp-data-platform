with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_annotation_bug_report") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            description,
            cast(submitted_at as timestamp) as submitted_at
        from source
    )
select *
from renamed
