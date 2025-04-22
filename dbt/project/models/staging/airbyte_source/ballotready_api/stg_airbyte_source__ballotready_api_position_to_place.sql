with
    source as (
        select *
        from {{ source("airbyte_source", "ballotready_api_position_to_place") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            from_json(
                places, 'STRUCT<nodes:ARRAY<STRUCT<databaseId:INT, id:STRING>>>'
            ) as places,
            to_timestamp(createdat) as created_at,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id

        from source
    )
select *
from renamed
