{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    clean_officeholders as (
        select
            office_holder_id,
            first_name,
            last_name,
            case
                when upper(is_incumbent) = 'TRUE'
                then 'Incumbent'
                when upper(is_incumbent) = 'FALSE'
                then 'Non-Incumbent'
            end as officeholder_type,
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
            normalized_location as city,
            state,
            office_name as official_office_name,
            office_normalized as official_office,
            office_type,
            office_level,
            position_id,
            normalized_position_id,
            `level`,
            tier,
            filing_deadline,
            primary_election_day as primary_election_date,
            general_election_day as general_election_date,
            date_processed,
            -- Transform is_uncontested to Uncontested
            case
                when upper(is_uncontested) = 'NO'
                then 'Contested'
                when upper(is_uncontested) = 'YES'
                then 'Uncontested'
                else is_uncontested
            end as uncontested,
            seats_available as number_of_seats_available,
            partisan,
            running_for_re_election_2025,
            running_for_re_election_2026,
            url_for_running_for_re_election_2025,
            url_for_running_for_re_election_2026,
            ts_status,
            ts_comment,

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
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
        {% if is_incremental() %}
            where
                _airbyte_extracted_at
                > (select max(_airbyte_extracted_at) from {{ this }})
        {% endif %}
    )
select
    office_holder_id,
    first_name,
    last_name,
    officeholder_type,
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
    state,
    official_office_name,
    official_office,
    office_type,
    office_level,
    position_id,
    normalized_position_id,
    `level`,
    tier,
    filing_deadline,
    primary_election_date,
    general_election_date,
    date_processed,
    uncontested,
    number_of_seats_available,
    partisan,
    running_for_re_election_2025,
    running_for_re_election_2026,
    url_for_running_for_re_election_2025,
    url_for_running_for_re_election_2026,
    ts_status,
    ts_comment,
    type,
    contact_owner,
    owner_name,
    uploaded,
    _ab_source_file_url,
    _airbyte_extracted_at
from clean_officeholders
