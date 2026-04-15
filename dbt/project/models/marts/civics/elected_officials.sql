-- Civics mart: elected_officials
-- Pass-through of BallotReady office holders. BallotReady is the first
-- (and currently only) provider for elected officials. Additional providers
-- (e.g., TechSpeed) will be added once entity resolution is in place.
-- Grain: one row per office-holder term.
with
    combined as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (
                partition by gp_elected_official_id order by updated_at desc
            )
            = 1
    )

select
    gp_elected_official_id,
    br_office_holder_id,
    br_position_id,
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
from deduplicated
