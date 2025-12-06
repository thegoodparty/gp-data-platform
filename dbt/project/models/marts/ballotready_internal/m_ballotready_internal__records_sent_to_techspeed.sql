/*
    Materializing this incrementally saves us < 3 seconds *when there are no new
    records to process*, so building the whole table until we know it's a
    performance bottleneck
*/
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
            sub_area_name_secondary,
            sub_area_value_secondary,
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
            geofence_is_not_exact,
            is_primary,
            is_runoff,
            is_unexpired,
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
            {{
                generate_candidate_office_from_position(
                    "position_name", "normalized_position_name"
                )
            }} as candidate_office,
            candidacy_created_at,
            candidacy_updated_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            -- Records with missing contact info that need TechSpeed enrichment
            (phone = '' or email = '')
            -- Remove major party candidates
            and not parties like '%Democrat%'
            and not parties like '%Republican%'
            -- Only future elections (at least 3 days out for TechSpeed processing)
            and election_day > current_date + interval 3 day
            -- Only records not already sent to TechSpeed
            and candidacy_id not in (
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
                    "first_name", "last_name", "state", "office_type"
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
    sub_area_name_secondary,
    sub_area_value_secondary,
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
    geofence_is_not_exact,
    is_primary,
    is_runoff,
    is_unexpired,
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
