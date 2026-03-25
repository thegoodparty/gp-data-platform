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
    -- Exclude: vacancies (no real person), blank names, state='US' (national)
    br_elected as (
        select *
        from {{ ref("int__civics_elected_official_ballotready") }}
        where
            coalesce(is_vacant, false) = false
            and nullif(trim(first_name), '') is not null
            and nullif(trim(last_name), '') is not null
            and state != 'US'
    ),

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
            {{ extract_district_raw("position_name") }} as district_raw,
            {{ extract_district_identifier("position_name") }} as district_identifier,
            email,
            -- Normalize to 10-digit US phone: strip country code prefix '1' if
            -- 11 digits, otherwise truncate (handles extensions like "ext. 2002")
            -- phone is already cleaned upstream via clean_phone_number macro
            case
                when length(phone) = 11 and phone like '1%'
                then substring(phone, 2, 10)
                else substring(phone, 1, 10)
            end as phone,
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
        where
            state is not null
            and nullif(trim(first_name), '') is not null
            and nullif(trim(last_name), '') is not null
    ),

    techspeed_officials as (
        select
            'techspeed' as source_name,
            -- gp_elected_official_id is a salted UUID from (ts_officeholder_id,
            -- position_id, office_name_clean) — the true unique key. Using the
            -- simpler ts_officeholder_id_position_id composite is NOT unique
            -- because TechSpeed reuses ID pairs across different people.
            cast(gp_elected_official_id as string) as source_id,
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
            case
                when length(phone) = 11 and phone like '1%'
                then substring(phone, 2, 10)
                else substring(phone, 1, 10)
            end as phone,
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
