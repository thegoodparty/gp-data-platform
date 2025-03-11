with

    source as (select * from {{ source("airbyte_source", "ballotready_api_mtfcc") }}),

    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            name,
            mtfcc,
            databaseid
        from source
    )

select *
from renamed
