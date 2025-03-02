with 

source as (

    select * from {{ source('airbyte_source', 'ballotready_api_election') }}

),

renamed as (

    select
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        _airbyte_generation_id,
        id,
        name,
        slug,
        state,
        timezone,
        createdat,
        racecount,
        updatedat,
        databaseid,
        milestones,
        electionday,
        vipelections,
        defaulttimezone,
        originalelectiondate,
        votinginformationpublishedat,
        candidateinformationpublishedat

    from source

)

select * from renamed
