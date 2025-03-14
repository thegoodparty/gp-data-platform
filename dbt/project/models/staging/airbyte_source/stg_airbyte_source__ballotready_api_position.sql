with

    source as (

        select * from {{ source("airbyte_source", "ballotready_api_position") }}

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
            cast(tier as int) as tier,
            cast(geoid as int) as geoid,
            level,
            mtfcc,
            cast(seats as int) as seats,
            state,
            from_json(issues, 'ARRAY<STRUCT<databaseId:INT, id:STRING>>') as issues,
            cast(salary as int) as salary,
            judicial,
            cast(roworder as int) as row_order,
            appointed,
            to_timestamp(createdat) as created_at_utc,
            retention,
            to_timestamp(updatedat) as updated_at_utc,
            cast(databaseid as int) as database_id,
            hasprimary as has_primary,
            cast(minimumage as float) as minimum_age,
            description,
            filingphone as filing_phone,
            subareaname as sub_area_name,
            partisantype as partisan_type,
            cast(subareavalue as int) as sub_area_value,
            filingaddress as filing_address,
            staggeredterm as staggered_term,
            employmenttype as employment_type,
            mustberesident as must_be_resident,
            cast(maximumfilingfee as int) as maximum_filing_fee,
            runningmatestyle as running_matrix_style,
            cast(selectionsallowed as int) as selections_allowed,
            filingrequirements as filing_requirements,
            from_json(
                normalizedposition, 'STRUCT<databaseId:INT, id:STRING>'
            ) as normalized_position,
            from_json(
                electionfrequencies, 'ARRAY<STRUCT<databaseId:INT, id:STRING>>'
            ) as election_frequencies,
            hasunknownboundaries as has_unknown_boundaries,
            mustberegisteredvoter as must_be_registered_voter,
            paperworkinstructions as paperwork_instructions,
            hasmajorityvoteprimary as has_majority_vote_primary,
            hasrankedchoicegeneral as has_ranked_choice_general,
            hasrankedchoiceprimary as has_ranked_choice_primary,
            from_json(
                eligibilityrequirements, 'ARRAY<STRUCT<databaseId:INT, id:STRING>>'
            ) as eligibility_requirements,
            cast(rankedchoicemaxvotesgeneral as int) as ranked_choice_max_votes_general,
            cast(rankedchoicemaxvotesprimary as int) as ranked_choice_max_votes_primary,
            musthaveprofessionalexperience as must_have_professional_experience

        from source

    )

select *
from renamed
