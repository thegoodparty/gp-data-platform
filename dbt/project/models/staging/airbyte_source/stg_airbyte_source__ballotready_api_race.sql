with

    source as (select * from {{ source("airbyte_source", "ballotready_api_race") }}),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            cast(seats as int) as seats,
            from_json(election, 'STRUCT<databaseId:INT, id:STRING>') as election,
            isrecall as is_recall,
            isrunoff as is_runoff,
            from_json(position, 'STRUCT<databaseId:INT, id:STRING>') as position,
            to_timestamp(createdat) as created_at,
            isprimary as is_primary,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id,
            isdisabled as is_disabled,
            ispartisan as is_partisan,
            from_json(
                candidacies, 'ARRAY<STRUCT<databaseId:INT, id:STRING>>'
            ) as candidacies,
            isunexpired as is_unexpired,
            filingperiods

        from source

    )

select *
from renamed
