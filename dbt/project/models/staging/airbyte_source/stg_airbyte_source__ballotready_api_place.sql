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
            from_json(urls, 'array<struct<databaseId:int,id:string>>') as urls,
            forms,
            geoid,
            mtfcc,
            state,
            from_json(
                contacts,
                'array<struct<email:string,fax:string,phone:string,type:string>>'
            ) as contacts,
            timezone,
            from_json(
                addresses, 'array<struct<databaseId:int,id:string>>'
            ) as addresses,
            to_timestamp(createdat) as created_at,
            cast(dissolved as boolean) as dissolved,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id,
            primarytype as primary_type,
            cast(hasvotebymail as boolean) as has_vote_by_mail,
            cast(isprintingenabled as boolean) as is_printing_enabled,
            from_json(
                registrationoptions, 'array<struct<databaseId:int,id:string>>'
            ) as registration_options,
            cast(
                isreceiverofvotebymailrequests as boolean
            ) as is_receiver_of_vote_by_mail_requests,
            cast(
                canvoteinprimarywhen18bygeneral as boolean
            ) as can_vote_in_primary_when_18_by_general

        from source

    )

select *
from renamed
