with
    candidacies as (
        select *
        from goodparty_data_catalog.airbyte_source.ballotready_s3_candidacies_v3
    ),

    us_states as (select * from {{ ref("us_states") }}),

    candidacies_cleaned as (
        select
            -- BallotReady identifiers
            id as br_id,
            geo_id as br_geo_id,
            race_id as br_race_id,
            election_id as br_election_id,
            geofence_id as br_geofence_id,
            position_id as br_position_id,
            normalized_position_id as br_normalized_position_id,
            candidate_id as br_candidate_id,
            candidacy_id as br_candidacy_id,

            candidacy_created_at,
            candidacy_updated_at,

            -- election-stage-level values
            nullif(trim(election_result), '') as result,
            is_primary = 'true' as is_primary,
            is_judicial = 'true' as is_judicial,
            is_runoff = 'true' as is_runoff,
            is_retention = 'true' as is_retention,
            case
                when is_runoff = 'true'
                then 'Runoff'
                when is_primary = 'true'
                then 'Primary'
                else 'General'
            end as election_type,
            election_day as election_date,
            election_name as election_stage_name,

            -- position values
            position_name,
            state as state_code,
            us_states.state_name,
            number_of_seats,
            normalized_position_name,
            sub_area_name,
            sub_area_value,
            {{
                generate_candidate_office_from_position(
                    "position_name", "normalized_position_name"
                )
            }} as candidate_office,
            {{ extract_city_from_office_name("position_name") }} as city,

            -- Candidacy level fields
            tier as civicengine_tier,
            urls as raw_urls,
            parse_json(replace(urls, '=>', ':'))::array<variant> as url_list,
            parties as raw_parties,
            parse_json(replace(parties, '=>', ':'))::array<variant> as parsed_parties,
            case
                when size(parsed_parties) = 1
                then parsed_parties[0]:name::string
                else 'Multiple Parties'
            end as party,
            (
                lower(party) like '%nonpartisan%'
                or lower(party) = 'independent'
                or lower(party) like '%no party%'
                or lower(party) like '%unaffiliated%'
            ) as is_independent_or_nonpartisan,
            (
                lower(party) like '%democrat%' or lower(party) like '%republican%'
            ) as is_major_party,
            level as office_level,
            initcap(level) as office_level_formatted,
            /*
MAF/TIGER Feature Class Code (MTFCC) is a 5-digit code assigned by the Census Bureau intended to classify and describe geographic objects or features. Values starting with X are from BallotReady's custom research.
    */
            mtfcc,

            -- Candidate-level fields
            first_name as candidate_first_name,
            middle_name as candidate_middle_name,
            last_name as candidate_last_name,
            suffix as candidate_suffix,
            nickname as candidate_nickname,
            image_url as candidate_image_url,
            nullif(lower(trim(email)), '') as candidate_email,
            nullif(replace(phone, '-', ''), '') as candidate_phone,

            _airbyte_raw_id,
            _airbyte_extracted_at,
            _ab_source_file_url,
            _ab_source_file_last_modified

        from candidacies
        left join us_states on upper(trim(candidacies.state)) = us_states.state_code
    )

select *
from candidacies_cleaned
