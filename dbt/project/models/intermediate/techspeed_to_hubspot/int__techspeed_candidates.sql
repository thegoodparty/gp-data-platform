{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    clean_candidates as (
        select
            candidate_id_source,
            first_name,
            last_name,
            case
                when is_incumbent
                then 'Incumbent'
                when not is_incumbent
                then 'Challenger'
            end as candidate_type,
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
            -- Transform is_primary to Election Type
            case
                when is_primary then 'Primary' when not is_primary then 'General'
            end as election_type,

            -- Transform is_uncontested to Uncontested
            case
                when is_uncontested
                then 'Uncontested'
                when not is_uncontested
                then 'Contested'
            end as uncontested,
            number_of_candidates,
            number_of_seats_available,
            is_open_seat,
            is_partisan,
            population,
            br_race_id,
            election_result,

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
    is_open_seat,
    is_partisan,
    population,
    br_race_id,
    election_result,
    type,
    contact_owner,
    owner_name,
    uploaded,
    _ab_source_file_url,
    _airbyte_extracted_at
from clean_candidates
