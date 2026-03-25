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
            cast(geoid as string) as geo_id,
            level,
            mtfcc,
            cast(seats as int) as seats,
            state,
            from_json(issues, 'ARRAY<STRUCT<databaseId:INT, id:STRING>>') as issues,
            salary,
            {{ cast_to_boolean("judicial") }} as is_judicial,
            cast(roworder as int) as row_order,
            {{ cast_to_boolean("appointed") }} as is_appointed,
            to_timestamp(createdat) as created_at,
            {{ cast_to_boolean("retention") }} as is_retention,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id,
            {{ cast_to_boolean("hasprimary") }} as has_primary,
            cast(minimumage as float) as minimum_age,
            description,
            filingphone as filing_phone,
            subareaname as sub_area_name,
            {{ cast_to_boolean("partisantype", ["partisan"], ["nonpartisan"]) }}
            as is_partisan,
            partisantype as partisan_type,
            cast(subareavalue as string) as sub_area_value,
            filingaddress as filing_address,
            {{ cast_to_boolean("staggeredterm") }} as is_staggered_term,
            employmenttype as employment_type,
            {{ cast_to_boolean("mustberesident") }} as must_be_resident,
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
            {{ cast_to_boolean("hasunknownboundaries") }} as has_unknown_boundaries,
            {{ cast_to_boolean("mustberegisteredvoter") }} as must_be_registered_voter,
            paperworkinstructions as paperwork_instructions,
            {{ cast_to_boolean("hasmajorityvoteprimary") }}
            as has_majority_vote_primary,
            {{ cast_to_boolean("hasrankedchoicegeneral") }}
            as has_ranked_choice_general,
            {{ cast_to_boolean("hasrankedchoiceprimary") }}
            as has_ranked_choice_primary,
            eligibilityrequirements as eligibility_requirements,
            cast(rankedchoicemaxvotesgeneral as int) as ranked_choice_max_votes_general,
            cast(rankedchoicemaxvotesprimary as int) as ranked_choice_max_votes_primary,
            {{ cast_to_boolean("musthaveprofessionalexperience") }}
            as must_have_professional_experience

        from source

    )

select *
from renamed
