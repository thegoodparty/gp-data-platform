with

    source as (select * from {{ source("airbyte_source", "ballotready_api_issue") }}),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            key,
            name,
            databaseid,
            pluginenabled

        from source

    )

select *
from renamed
