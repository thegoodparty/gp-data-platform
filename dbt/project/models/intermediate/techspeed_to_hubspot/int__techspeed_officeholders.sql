{{ config(materialized="view") }}

with
    clean_officeholders as (
        select
            office_holder_id as ts_officeholder_id,
            first_name,
            last_name,
            case
                when lower(trim(is_incumbent)) in ('true', 'yes', '1')
                then true
                when lower(trim(is_incumbent)) in ('false', 'no', '0')
                then false
            end as is_incumbent,
            email,
            replace(
                replace(replace(replace(phone, '-', ''), '_', ''), '[', ''), ']', ''
            ) as phone,
            email_source,
            phone_source,
            party,
            street_address,
            postal_code,
            county_municipality,
            district_name as district,
            city,
            normalized_location,
            state,
            office_name as official_office_name,
            office_normalized as official_office,
            office_type,
            office_level,
            position_id,
            normalized_position_id,
            tier,
            filing_deadline,
            primary_election_day as primary_election_date,
            general_election_day as general_election_date,
            date_processed,
            case
                when lower(trim(is_uncontested)) in ('true', 'yes', '1')
                then true
                when lower(trim(is_uncontested)) in ('false', 'no', '0')
                then false
            end as is_uncontested,
            seats_available as number_of_seats_available,
            partisan,
            running_for_re_election_2025,
            running_for_re_election_2026,
            url_for_running_for_re_election_2025,
            url_for_running_for_re_election_2026,
            ts_status,
            ts_comment,

            -- placeholder for "uploaded" column
            case
                when (phone is null and email is null) then 'no_contact' else null
            end as uploaded,

            _ab_source_file_url,
            _airbyte_extracted_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
    )
select
    ts_officeholder_id,
    first_name,
    last_name,
    is_incumbent,
    email,
    phone,
    email_source,
    phone_source,
    party,
    street_address,
    postal_code,
    county_municipality,
    district,
    city,
    normalized_location,
    state,
    official_office_name,
    official_office,
    office_type,
    office_level,
    position_id,
    normalized_position_id,
    tier,
    filing_deadline,
    primary_election_date,
    general_election_date,
    date_processed,
    is_uncontested,
    number_of_seats_available,
    partisan,
    running_for_re_election_2025,
    running_for_re_election_2026,
    url_for_running_for_re_election_2025,
    url_for_running_for_re_election_2026,
    ts_status,
    ts_comment,
    uploaded,
    _ab_source_file_url,
    _airbyte_extracted_at
from clean_officeholders
