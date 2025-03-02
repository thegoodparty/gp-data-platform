with 

source as (

    select * from {{ source('airbyte_source', 'ballotready_api_race') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        _airbyte_generation_id,
        id,
        seats,
        election,
        isrecall,
        isrunoff,
        position,
        createdat,
        isprimary,
        updatedat,
        databaseid,
        isdisabled,
        ispartisan,
        candidacies,
        isunexpired,
        filingperiods

    from source

)

select * from renamed
