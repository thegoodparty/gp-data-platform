with 

source as (

    select * from {{ source('airbyte_source', 'ballotready_api_position') }}

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
        tier,
        geoid,
        level,
        mtfcc,
        seats,
        state,
        issues,
        salary,
        judicial,
        roworder,
        appointed,
        createdat,
        retention,
        updatedat,
        databaseid,
        hasprimary,
        minimumage,
        description,
        filingphone,
        subareaname,
        partisantype,
        subareavalue,
        filingaddress,
        staggeredterm,
        employmenttype,
        mustberesident,
        maximumfilingfee,
        runningmatestyle,
        selectionsallowed,
        filingrequirements,
        normalizedposition,
        electionfrequencies,
        hasunknownboundaries,
        mustberegisteredvoter,
        paperworkinstructions,
        hasmajorityvoteprimary,
        hasrankedchoicegeneral,
        hasrankedchoiceprimary,
        eligibilityrequirements,
        rankedchoicemaxvotesgeneral,
        rankedchoicemaxvotesprimary,
        musthaveprofessionalexperience

    from source

)

select * from renamed
