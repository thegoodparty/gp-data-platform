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
            from_json(election, 'struct<databaseId:int,id:string>') as election,
            cast(isrecall as boolean) as is_recall,
            cast(isrunoff as boolean) as is_runoff,
            from_json(position, 'struct<databaseId:int,id:string>') as position,
            to_timestamp(createdat) as created_at,
            cast(isprimary as boolean) as is_primary,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id,
            cast(isdisabled as boolean) as is_disabled,
            cast(ispartisan as boolean) as is_partisan,
            from_json(
                candidacies, 'array<struct<databaseId:int,id:string>>'
            ) as candidacies,
            cast(isunexpired as boolean) as is_unexpired,
            from_json(
                filingperiods, 'array<struct<databaseId:int,id:string>>'
            ) as filing_periods

        from source

    )

select *
from renamed
