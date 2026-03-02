{{ config(materialized="table", tags=["civics", "entity_resolution"]) }}

-- Entity Resolution prematch: BallotReady x TechSpeed candidacies
-- Unions candidacies from both sources into a standardized schema for Splink matching.
--
-- Grain: One row per source candidacy record
-- Key: unique_id (source_name || '|' || source_id)
--
-- Name cleaning is intentionally minimal (lower/trim) — more sophisticated
-- parsing (e.g. HumanName for suffixes/nicknames) happens in the Python
-- Splink script to keep iteration fast without rebuilding the dbt model.
--
-- District/city/seat_name for BallotReady are derived from official_office_name
-- using the same regex patterns as int__civics_candidacy_ballotready. For
-- TechSpeed these come from int__techspeed_candidates_clean.
with
    -- BallotReady candidacies (has standardized office, party, IDs, dates)
    br_candidacy as (select * from {{ ref("int__civics_candidacy_ballotready") }}),

    -- BallotReady candidates (name, email, phone)
    br_candidate as (select * from {{ ref("int__civics_candidate_ballotready") }}),

    ballotready_candidacies as (
        select
            'ballotready' as source_name,
            br_candidacy.br_candidacy_id as source_id,
            br_candidacy.gp_candidacy_id,
            lower(trim(br_candidate.first_name)) as first_name,
            lower(trim(br_candidate.last_name)) as last_name,
            br_candidate.state,
            br_candidacy.party_affiliation as party,
            br_candidacy.candidate_office,
            br_candidacy.office_level,
            br_candidacy.office_type,
            -- Derive district from official_office_name (same regex as candidacy model)
            coalesce(
                regexp_extract(
                    br_candidacy.official_office_name,
                    '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
                ),
                ''
            ) as district_raw,
            regexp_extract(
                br_candidacy.official_office_name,
                '- (?:District|Ward|Place|Branch|Subdistrict|Zone) ([0-9]+)'
            ) as district_identifier,
            {{ extract_city_from_office_name("br_candidacy.official_office_name") }}
            as city,
            br_candidacy.general_election_date,
            br_candidacy.primary_election_date,
            coalesce(
                br_candidacy.general_election_date,
                br_candidacy.primary_election_date,
                br_candidacy.runoff_election_date
            ) as election_date,
            br_candidate.email,
            br_candidate.phone_number as phone,
            br_candidacy.br_race_id,
            br_candidacy.official_office_name,
            -- Derive seat_name from official_office_name (same regex as candidacy
            -- model)
            coalesce(
                regexp_extract(
                    br_candidacy.official_office_name, '[-, ] (?:Seat|Group) ([^,]+)'
                ),
                regexp_extract(
                    br_candidacy.official_office_name, ' - Position ([^\\s(]+)'
                ),
                ''
            ) as seat_name
        from br_candidacy
        inner join
            br_candidate on br_candidacy.gp_candidate_id = br_candidate.gp_candidate_id
    ),

    -- TechSpeed candidacies (has standardized office, party, IDs, dates)
    -- Filter to 2026+ to match BallotReady scope (BR is already filtered upstream)
    ts_candidacy as (
        select *
        from {{ ref("int__civics_candidacy_techspeed") }}
        where general_election_date >= '2026-01-01'
    ),

    -- TechSpeed candidates (name, email, phone)
    ts_candidate as (select * from {{ ref("int__civics_candidate_techspeed") }}),

    -- TechSpeed source for fields not in candidacy intermediate
    -- (br_race_id, district, city, office_type)
    ts_source as (
        select techspeed_candidate_code, br_race_id, district, city, office_type
        from {{ ref("int__techspeed_candidates_clean") }}
    ),

    techspeed_candidacies as (
        select
            'techspeed' as source_name,
            ts_candidacy.candidate_code as source_id,
            ts_candidacy.gp_candidacy_id,
            lower(trim(ts_candidate.first_name)) as first_name,
            lower(trim(ts_candidate.last_name)) as last_name,
            ts_candidate.state,
            ts_candidacy.party_affiliation as party,
            ts_candidacy.candidate_office,
            ts_candidacy.office_level,
            ts_source.office_type,
            ts_source.district as district_raw,
            regexp_extract(ts_source.district, '([0-9]+)') as district_identifier,
            ts_source.city,
            ts_candidacy.general_election_date,
            ts_candidacy.primary_election_date,
            coalesce(
                ts_candidacy.general_election_date, ts_candidacy.primary_election_date
            ) as election_date,
            ts_candidate.email,
            ts_candidate.phone_number as phone,
            ts_source.br_race_id,
            ts_candidacy.official_office_name,
            cast(null as string) as seat_name
        from ts_candidacy
        inner join
            ts_candidate on ts_candidacy.gp_candidate_id = ts_candidate.gp_candidate_id
        left join
            ts_source
            on ts_candidacy.candidate_code = ts_source.techspeed_candidate_code
    ),

    unioned as (
        select *
        from ballotready_candidacies
        union all
        select *
        from techspeed_candidacies
    )

select
    source_name || '|' || source_id as unique_id,
    source_id,
    source_name,
    gp_candidacy_id,
    first_name,
    last_name,
    state,
    party,
    candidate_office,
    office_level,
    office_type,
    district_raw,
    district_identifier,
    city,
    general_election_date,
    primary_election_date,
    election_date,
    email,
    phone,
    br_race_id,
    official_office_name,
    seat_name
from unioned
