{{ config(materialized="table", tags=["civics", "entity_resolution"]) }}

-- Entity Resolution prematch: BallotReady x TechSpeed elected officials
-- Unions officeholder records from both sources into a standardized schema
-- for Splink probabilistic matching.
--
-- Grain: One row per office-holder term (BR) / officeholder-position (TS)
-- Key: unique_id (source_name || '|' || source_id)
--
-- Unlike the candidacy prematch, elected officials have no election_date or
-- br_race_id. Splink blocking will anchor on state + office name + last name
-- and contact info (phone, email).
--
with
    -- Nickname aliases: aggregate nicknames per canonical name into an array
    -- so Splink can use ArrayIntersectLevel to detect nickname matches
    -- (e.g. robert ↔ bob).
    nickname_aliases as (
        select
            name1, array_distinct(array_append(collect_list(name2), name1)) as aliases
        from {{ ref("nicknames") }}
        group by name1
    ),

    -- BallotReady elected officials (from intermediate)
    -- All historical data included
    br_elected as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    ballotready_officials as (
        select
            'ballotready' as source_name,
            cast(br_office_holder_id as string) as source_id,
            lower(trim(first_name)) as first_name,
            lower(trim(last_name)) as last_name,
            state,
            party_affiliation as party,
            candidate_office,
            office_level,
            office_type,
            district as district_raw,
            -- Derive numeric district from position_name (same pattern as candidacy
            -- prematch)
            coalesce(
                try_cast(
                    regexp_extract(
                        position_name,
                        '- (?:District|Ward|Place|Branch|Subdistrict|Zone|Precinct|Position|Area|Region|Circuit|Division|Post|Section|Subdivision|Seat) ([0-9]+)'
                    ) as int
                ),
                try_cast(
                    regexp_extract(
                        position_name, '([0-9]+)(?:st|nd|rd|th) Congressional'
                    ) as int
                )
            ) as district_identifier,
            email,
            -- Truncate to 10 digits: strips country code and extensions
            -- (e.g. "603-868-5100 ext. 2002" → "6038685100")
            substring({{ clean_phone_number("phone") }}, 1, 10) as phone,
            position_name as official_office_name,
            city,
            term_start_date,
            term_end_date,
            br_office_holder_id,
            br_candidate_id,
            cast(null as string) as ts_officeholder_id,
            cast(null as string) as ts_position_id
        from br_elected
    ),

    -- TechSpeed elected officials (from intermediate)
    ts_elected as (
        select *
        from {{ ref("int__civics_elected_official_techspeed") }}
        where state is not null
    ),

    techspeed_officials as (
        select
            'techspeed' as source_name,
            ts_officeholder_id
            || '_'
            || coalesce(ts_position_id, '')
            || '_'
            || coalesce(trim(position_name), '') as source_id,
            lower(trim(first_name)) as first_name,
            lower(trim(last_name)) as last_name,
            state,
            party_affiliation as party,
            candidate_office,
            office_level,
            office_type,
            district as district_raw,
            try_cast(
                regexp_extract(district, '([0-9]+)') as int
            ) as district_identifier,
            email,
            substring({{ clean_phone_number("phone") }}, 1, 10) as phone,
            position_name as official_office_name,
            city,
            cast(null as date) as term_start_date,
            cast(null as date) as term_end_date,
            cast(null as int) as br_office_holder_id,
            cast(null as int) as br_candidate_id,
            ts_officeholder_id,
            ts_position_id
        from ts_elected
    ),

    unioned as (
        select *
        from ballotready_officials
        union all
        select *
        from techspeed_officials
    )

-- Splink treats empty strings as real values for exact matching, so convert
-- sparse columns to NULL where empty so missing data is handled correctly.
select
    source_name || '|' || source_id as unique_id,
    source_id,
    u.source_name,
    u.first_name,
    u.last_name,
    -- Array of first_name + all known nicknames for Splink ArrayIntersectLevel
    coalesce(na.aliases, array(u.first_name)) as first_name_aliases,
    u.state,
    party,
    candidate_office,
    office_level,
    office_type,
    nullif(district_raw, '') as district_raw,
    district_identifier,
    nullif(email, '') as email,
    nullif(phone, '') as phone,
    nullif(lower(trim(official_office_name)), '') as official_office_name,
    nullif(city, '') as city,
    term_start_date,
    term_end_date,
    br_office_holder_id,
    br_candidate_id,
    ts_officeholder_id,
    ts_position_id
from unioned as u
left join nickname_aliases as na on u.first_name = na.name1
