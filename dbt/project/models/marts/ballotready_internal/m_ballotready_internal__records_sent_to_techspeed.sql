{{
    config(
        materialized="table",
        tags=["mart", "ballotready", "techspeed", "historical"],
    )
}}

-- Historical tracking table for BallotReady records sent to TechSpeed
-- This table maintains an audit trail of all candidates that have been sent
-- to TechSpeed for data enrichment (missing phone/email)
with
    br_for_techspeed as (
        select
            br_id as id,
            br_candidacy_id as candidacy_id,
            br_election_id as election_id,
            election_stage_name as election_name,
            election_date as election_day,
            br_position_id as position_id,
            mtfcc,
            br_geo_id as geo_id,
            position_name,
            position_name as official_office_name,
            sub_area_name,
            sub_area_value,
            state_code as state,
            office_level as level,
            civicengine_tier as tier,
            is_judicial,
            is_retention,
            number_of_seats,
            br_normalized_position_id as normalized_position_id,
            normalized_position_name,
            br_race_id as race_id,
            br_geofence_id as geofence_id,
            is_primary,
            is_runoff,
            br_candidate_id as candidate_id,
            candidate_first_name as first_name,
            candidate_middle_name as middle_name,
            candidate_nickname as nickname,
            candidate_last_name as last_name,
            candidate_suffix as suffix,
            candidate_phone as phone,
            candidate_email as email,
            candidate_image_url as image_url,
            raw_parties as parties,
            raw_urls as urls,
            result as election_result,
            candidate_office,
            city,
            candidacy_created_at,
            candidacy_updated_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            -- Records with missing contact info that need TechSpeed enrichment
            (candidate_phone is null or candidate_email is null)
            -- Remove major party candidates
            and not is_major_party
            -- Only future elections (at least 3 days out for TechSpeed processing)
            and election_date > current_date + interval 3 day
            -- Only records not already sent to TechSpeed
            and br_candidacy_id not in (
                select candidacy_id
                from {{ ref("stg_historical__ballotready_records_sent_to_techspeed") }}
            )
    ),
    with_office_type as (
        select *, {{ map_ballotready_office_type("candidate_office") }} as office_type
        from br_for_techspeed
    ),
    with_candidate_code as (
        select
            *,
            {{
                generate_candidate_code(
                    "first_name", "last_name", "state", "office_type", "city"
                )
            }} as candidate_code
        from with_office_type
    ),
    dedup_from_hubspot as (
        select *
        from with_candidate_code
        -- Omit existing candidates already present in Hubspot from BR subset to be
        -- shared with TechSpeed
        where
            candidate_code not in (
                select hubspot_candidate_code
                from {{ ref("int__hubspot_ytd_candidacies") }}
                where hubspot_candidate_code is not null
            )
    )
select
    id,
    candidacy_id,
    election_id,
    election_name,
    election_day,
    position_id,
    mtfcc,
    geo_id,
    position_name,
    sub_area_name,
    sub_area_value,
    state,
    level,
    tier,
    is_judicial,
    is_retention,
    number_of_seats,
    normalized_position_id,
    normalized_position_name,
    race_id,
    geofence_id,
    is_primary,
    is_runoff,
    candidate_id,
    first_name,
    middle_name,
    nickname,
    last_name,
    suffix,
    phone,
    email,
    image_url,
    parties,
    urls,
    election_result,
    candidacy_created_at,
    candidacy_updated_at,
    current_timestamp() as upload_datetime
from dedup_from_hubspot
