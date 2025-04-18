with

    source as (

        select * from {{ source("airbyte_source", "ballotready_api_election") }}

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
            to_timestamp(createdat) as created_at,
            cast(racecount as int) as race_count,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id,
            from_json(
                milestones,
                'ARRAY<STRUCT<category:STRING, channel:STRING, date:DATE, datetime:TIMESTAMP, features:ARRAY<STRUCT<type:STRING>>, type:STRING>>'
            ) as milestones,
            cast(electionday as date) as election_day,
            from_json(
                vipelections, 'ARRAY<STRUCT<party:STRING, vipId:INT>>'
            ) as vipelections,
            defaulttimezone as default_timezone,
            cast(originalelectiondate as date) as original_election_date,
            to_timestamp(
                votinginformationpublishedat
            ) as voting_information_published_at,
            to_timestamp(
                candidateinformationpublishedat
            ) as candidate_information_published_at

        from source

    )

select *
from renamed
