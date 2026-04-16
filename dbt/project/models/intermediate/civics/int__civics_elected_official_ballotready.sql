-- BallotReady office holders -> Civics mart elected_officials schema
-- Source: stg_airbyte_source__ballotready_s3_office_holders_v3
--
-- Grain: One row per office-holder term.
--
with
    office_holders as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
    ),

    derived_fields as (
        select
            office_holders.*,
            concat(first_name, ' ', last_name) as full_name,
            {{
                generate_candidate_office_from_position(
                    "position_name",
                    "normalized_position_name",
                )
            }} as candidate_office,
            case
                when lower(level) = 'city' then 'Local' else initcap(level)
            end as office_level,
            {{ extract_city_from_office_name("position_name") }} as city,
            {{ extract_district_geographic("position_name") }} as district,
            case
                when get(party_names, 0) is null
                then null
                else {{ parse_party_affiliation("get(party_names, 0)") }}
            end as party_affiliation
        from office_holders
    ),

    elected_officials as (
        select
            -- BallotReady's office_holder_id is the natural key for the
            -- office-holder term grain and avoids collisions across positions.
            {{
                generate_salted_uuid(
                    fields=[
                        "'ballotready_office_holder'",
                        "cast(br_office_holder_id as string)",
                    ]
                )
            }} as gp_elected_official_id,
            br_office_holder_id,
            br_candidate_id,
            br_position_id,
            br_candidacy_id,
            first_name,
            last_name,
            middle_name,
            suffix,
            full_name,
            email,
            {{ clean_phone_number("phone") }} as phone,
            {{ clean_phone_number("office_phone") }} as office_phone,
            {{ clean_phone_number("central_phone") }} as central_phone,
            position_name,
            normalized_position_name,
            candidate_office,
            office_level,
            {{ map_office_type("candidate_office") }} as office_type,
            state,
            city,
            district,
            start_at as term_start_date,
            end_at as term_end_date,
            is_appointed,
            is_judicial,
            is_vacant,
            is_off_cycle,
            party_affiliation,
            website_url,
            linkedin_url,
            facebook_url,
            twitter_url,
            nullif(office_holder_mailing_address_line_1, '') as mailing_address_line_1,
            nullif(office_holder_mailing_address_line_2, '') as mailing_address_line_2,
            nullif(office_holder_mailing_city, '') as mailing_city,
            nullif(office_holder_mailing_state, '') as mailing_state,
            nullif(office_holder_mailing_zip, '') as mailing_zip,
            br_geo_id,
            br_position_tier,
            'ballotready' as candidate_id_source,
            office_holder_created_at as created_at,
            office_holder_updated_at as updated_at
        from derived_fields
    )

select
    gp_elected_official_id,
    br_office_holder_id,
    br_candidate_id,
    br_position_id,
    br_candidacy_id,
    first_name,
    last_name,
    middle_name,
    suffix,
    full_name,
    email,
    phone,
    office_phone,
    central_phone,
    position_name,
    normalized_position_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    term_start_date,
    term_end_date,
    is_appointed,
    is_judicial,
    is_vacant,
    is_off_cycle,
    party_affiliation,
    website_url,
    linkedin_url,
    facebook_url,
    twitter_url,
    mailing_address_line_1,
    mailing_address_line_2,
    mailing_city,
    mailing_state,
    mailing_zip,
    br_geo_id,
    br_position_tier,
    candidate_id_source,
    created_at,
    updated_at
from elected_officials
