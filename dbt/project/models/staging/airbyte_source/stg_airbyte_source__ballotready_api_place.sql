with

    source as (select * from {{ source("airbyte_source", "ballotready_api_place") }}),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            name,
            slug,
            urls,
            forms,
            geoid as geo_id,
            mtfcc,
            state,
            contacts,
            timezone,
            addresses,
            createdat,
            dissolved,
            updatedat,
            databaseid,
            primarytype,
            hasvotebymail,
            isprintingenabled,
            registrationoptions,
            isreceiverofvotebymailrequests,
            canvoteinprimarywhen18bygeneral

        from source

    )

select *
from renamed
