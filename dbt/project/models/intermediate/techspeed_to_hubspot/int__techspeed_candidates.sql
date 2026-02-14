{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    clean_candidates as (
        select
            candidate_id_source,
            first_name,
            last_name,
            case
                when upper(is_incumbent) = 'TRUE'
                then 'Incumbent'
                when upper(is_incumbent) = 'FALSE'
                then 'Challenger'
            end as candidate_type,
            email,
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
            -- Transform is_primary to Election Type
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

            -- Transform is_uncontested to Uncontested
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
            br_race_id,

            -- Assign constant values
            'Self-Filer Lead' as `type`,
            'jesse@goodparty.org' as contact_owner,
            'Jesse Diliberto' as owner_name,

            -- placeholder for "uploaded" column
            case
                when (phone is null and email is null) then 'no_contact' else null
            end as uploaded,

            _ab_source_file_url,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
        {% if is_incremental() %}
            where
                _airbyte_extracted_at
                > (select max(_airbyte_extracted_at) from {{ this }})
        {% endif %}
    )
select
    candidate_id_source,
    first_name,
    last_name,
    candidate_type,
    email,
    phone,
    candidate_id_tier,
    party,
    website_url,
    linkedin_url,
    instagram_handle,
    twitter_handle,
    facebook_url,
    birth_date,
    street_address,
    postal_code,
    district,
    city,
    state,
    official_office_name,
    candidate_office,
    office_type,
    office_level,
    filing_deadline,
    primary_election_date,
    general_election_date,
    election_type,
    uncontested,
    number_of_candidates,
    number_of_seats_available,
    open_seat,
    partisan,
    population,
    br_race_id,
    type,
    contact_owner,
    owner_name,
    uploaded,
    _ab_source_file_url,
    _airbyte_extracted_at
from clean_candidates
