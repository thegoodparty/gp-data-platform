{{ config(materialized="view", tags=["techspeed", "cleaning"]) }}

with
    new_candidates as (select * from {{ ref("int__techspeed_new_candidates") }}),

    -- Initial cleaning and field standardization
    initial_cleaning as (
        select
            candidate_id_source,
            first_name,
            -- Apply name cleaning macro to last name
            {{ clean_candidate_names("last_name") }} as last_name,

            -- Standardize candidate type
            case
                when upper(is_incumbent) = 'TRUE'
                then 'Incumbent'
                when upper(is_incumbent) = 'FALSE'
                then 'Challenger'
                else null
            end as candidate_type,

            email,

            -- Clean phone number by removing common separators
            replace(
                replace(replace(replace(phone, '-', ''), '_', ''), '[', ''), ']', ''
            ) as phone,

            candidate_id_tier,
            party,
            website_url,
            linkedin_url,
            instagram_handle,
            twitter_handle,
            facebook_url,
            date_of_birth_mmddyyyy as birth_date,
            street_address,
            postal_code,
            district_name as district,
            normalized_location as city,
            state,
            office_name as official_office_name,
            office_normalized as candidate_office,
            office_type,
            office_level,
            filing_deadline,
            primary_election_date,
            general_election_date,

            -- Transform election type
            case
                when
                    (
                        upper(is_primary) = 'YES'
                        or upper(is_primary) = 'TRUE'
                        or upper(is_primary) = 'PRIMARY'
                    )
                then 'Primary'
                when
                    (
                        upper(is_primary) = 'NO'
                        or upper(is_primary) = 'FALSE'
                        or upper(is_primary) = 'GENERAL'
                    )
                then 'General'
                else is_primary
            end as election_type,

            -- Transform contested status
            case
                when upper(is_uncontested) = 'NO'
                then 'Contested'
                when upper(is_uncontested) = 'YES'
                then 'Uncontested'
                else is_uncontested
            end as uncontested,

            number_candidates as number_of_candidates,
            seats_available as number_of_seats_available,
            open_seat,
            partisan,
            population,
            ballotready_race_id,

            -- Assign constant values for HubSpot
            'Self-Filer Lead' as type,
            'jesse@goodparty.org' as contact_owner,
            'Jesse Diliberto' as owner_name,

            -- Preserve source file info for tracking
            _ab_source_file_url

        from new_candidates
    ),

    -- Filter out records without contact information
    contactable_candidates as (
        select
            *,
            case
                when (first_name is null or trim(first_name) = '') then 'no_first_name'
                when (last_name is null or trim(last_name) = '') then 'no_last_name'  
                when ((phone is null or trim(phone) = '') and (email is null or trim(email) = '')) then 'no_contact'
                else null
            end as upload_exclusion_reason
        from initial_cleaning
    )

select *
from contactable_candidates
