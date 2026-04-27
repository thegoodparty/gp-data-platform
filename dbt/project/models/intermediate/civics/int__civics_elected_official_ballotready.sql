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
            initcap(level) as office_level,
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
    ),

    -- BR candidacies keyed on br_candidacy_id, providing the election_day for
    -- the candidacy that produced each EO term. Used by the ICP effective-date
    -- gate. br_candidacy_id is unique on the source staging (291,028 rows,
    -- 291,028 distinct). Cast to int to match the EO type.
    candidacy_election_dates as (
        select cast(br_candidacy_id as int) as br_candidacy_id, election_day
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    -- ICP flags joined from int__icp_offices on br_position_id, with the Win
    -- effective-date gate driven by the candidacy's election_day rather than
    -- term_start_date. Election day is the canonical date for "when did this
    -- person win" — term_start_date is a noisy proxy that lags by weeks-to-
    -- months and can differ for appointed/special-election cases.
    --
    -- NULL handling:
    -- * NULL election_day (no candidacy match in BR's S3 export) → NULL flag.
    -- Only ~24% of EO rows have a matching candidacy, so most rows fall
    -- here. "We don't know when they won" is more truthful than "false".
    -- * NULL voter_count upstream → NULL flag (carried through from
    -- int__icp_offices).
    --
    -- is_serve_icp has no effective-date gate (Serve targets currently-seated
    -- officials regardless of when they were elected).
    with_icp_flags as (
        select
            eo.*,
            case
                when icp.icp_win_effective_date is null
                then icp.icp_office_win
                when ced.election_day is null
                then null
                when ced.election_day < icp.icp_win_effective_date
                then false
                else icp.icp_office_win
            end as is_win_icp,
            icp.icp_office_serve as is_serve_icp,
            case
                when icp.icp_win_effective_date is null
                then icp.icp_win_supersize
                when ced.election_day is null
                then null
                when ced.election_day < icp.icp_win_effective_date
                then false
                else icp.icp_win_supersize
            end as is_win_supersize_icp
        from elected_officials as eo
        left join
            candidacy_election_dates as ced on eo.br_candidacy_id = ced.br_candidacy_id
        left join
            {{ ref("int__icp_offices") }} as icp
            on eo.br_position_id = icp.br_database_position_id
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
    is_win_icp,
    is_serve_icp,
    is_win_supersize_icp,
    created_at,
    updated_at
from with_icp_flags
