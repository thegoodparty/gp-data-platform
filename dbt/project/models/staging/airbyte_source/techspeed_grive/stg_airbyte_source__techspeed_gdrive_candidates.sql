with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_candidates") }}
    ),

    renamed as (
        select
            -- Airbyte metadata
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,

            -- Candidate identity
            trim(first_name) as first_name,
            trim(last_name) as last_name,
            trim(state) as state,
            email,
            {{ clean_phone_number("phone") }} as phone,
            date_of_birth_mmddyyyy as birth_date,
            street_address,
            postal_code,

            -- Office / position
            trim(office_name) as official_office_name,
            trim(office_normalized) as candidate_office,
            office_type,
            office_level,
            trim(district_name) as district,
            trim(normalized_location) as city,
            county_municipality,

            -- Election
            replace(primary_election_date, '/', '-') as primary_election_date,
            replace(general_election_date, '/', '-') as general_election_date,
            replace(filing_deadline, '/', '-') as filing_deadline,
            election_result,

            -- Race metadata (booleans via cast_to_boolean from PR #280)
            party,
            {{ cast_to_boolean("partisan", ["partisan"], ["nonpartisan"]) }}
            as is_partisan,
            {{ cast_to_boolean("is_incumbent") }} as is_incumbent,
            {{ cast_to_boolean("is_primary") }} as is_primary,
            {{ cast_to_boolean("is_uncontested") }} as is_uncontested,
            {{ cast_to_boolean("open_seat") }} as is_open_seat,
            is_veteran,
            population,
            number_candidates as number_of_candidates,
            seats_available as number_of_seats_available,
            ballotready_race_id as br_race_id,

            -- Source tracking
            candidate_id_source,
            candidate_id_tier,
            ts_found_race_net_new,
            ts_found_candidate_net_new,
            ts_status,
            ts_comment,

            -- Contact sourcing
            phone_clean,
            phone_source,
            phone_type_select_1,
            email_source,
            source_url,
            date_processed,

            -- Social / web
            website_url,
            facebook_url,
            linkedin_url,
            twitter_handle,
            instagram_handle,

            -- Airbyte source file metadata
            _ab_source_file_url,
            _ab_source_file_last_modified

        from source
    ),

    invalid as (
        select _airbyte_raw_id
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates_invalid") }}
    )

select *
from renamed
where
    _airbyte_raw_id
    not in (select _airbyte_raw_id from invalid where _airbyte_raw_id is not null)
