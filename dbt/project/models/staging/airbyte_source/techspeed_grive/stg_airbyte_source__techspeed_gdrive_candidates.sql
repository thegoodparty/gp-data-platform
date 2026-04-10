with
    source as (
        select * from {{ source("airbyte_source", "techspeed_gdrive_candidates") }}
    ),

    clean_states as (select * from {{ ref("clean_states") }}),

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
            trim(regexp_replace(src.state, '[^A-Za-z ]', '')) as state,
            coalesce(
                cs.state_cleaned_postal_code,
                trim(regexp_replace(src.state, '[^A-Za-z ]', ''))
            ) as state_postal_code,
            email,
            {{ clean_phone_number("phone") }} as phone,
            date_of_birth_mmddyyyy as birth_date,
            coalesce(
                try_cast(date_of_birth_mmddyyyy as date),
                try_to_date(date_of_birth_mmddyyyy, 'MM-dd-yyyy'),
                try_to_date(date_of_birth_mmddyyyy, 'MM/dd/yyyy'),
                try_to_date(date_of_birth_mmddyyyy, 'yyyy-MM-dd')
            ) as birth_date_parsed,
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

            -- Election dates (raw strings with slash→dash normalization)
            replace(primary_election_date, '/', '-') as primary_election_date,
            replace(general_election_date, '/', '-') as general_election_date,
            replace(filing_deadline, '/', '-') as filing_deadline,
            -- Parsed DATE columns
            coalesce(
                try_cast(replace(primary_election_date, '/', '-') as date),
                try_to_date(replace(primary_election_date, '/', '-'), 'MM-dd-yyyy'),
                try_to_date(replace(primary_election_date, '/', '-'), 'MM-dd-yy')
            ) as primary_election_date_parsed,
            coalesce(
                try_cast(replace(general_election_date, '/', '-') as date),
                try_to_date(replace(general_election_date, '/', '-'), 'MM-dd-yyyy'),
                try_to_date(replace(general_election_date, '/', '-'), 'MM-dd-yy')
            ) as general_election_date_parsed,
            case
                when
                    year(
                        coalesce(
                            try_cast(replace(filing_deadline, '/', '-') as date),
                            try_to_date(
                                replace(filing_deadline, '/', '-'), 'MM-dd-yyyy'
                            )
                        )
                    )
                    between 1900 and 2050
                then
                    coalesce(
                        try_cast(replace(filing_deadline, '/', '-') as date),
                        try_to_date(replace(filing_deadline, '/', '-'), 'MM-dd-yyyy')
                    )
            end as filing_deadline_parsed,
            -- Coalesced election date (general preferred, fallback to primary)
            coalesce(
                try_cast(replace(general_election_date, '/', '-') as date),
                try_to_date(replace(general_election_date, '/', '-'), 'MM-dd-yyyy'),
                try_to_date(replace(general_election_date, '/', '-'), 'MM-dd-yy'),
                try_cast(replace(primary_election_date, '/', '-') as date),
                try_to_date(replace(primary_election_date, '/', '-'), 'MM-dd-yyyy'),
                try_to_date(replace(primary_election_date, '/', '-'), 'MM-dd-yy')
            ) as election_date,
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
            try_cast(
                trim(regexp_replace(cast(population as string), '[^0-9]', '')) as int
            ) as population,
            number_candidates as number_of_candidates,
            try_cast(seats_available as int) as seats_available,
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

        from source as src
        left join
            clean_states as cs
            on upper(trim(regexp_replace(src.state, '[^A-Za-z ]', '')))
            = upper(trim(cs.state_raw))
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
