{{ config(materialized="view", tags=["civics", "entity_resolution"]) }}

-- Entity Resolution prematch: BallotReady x DDHQ x TechSpeed election stages
-- (races/contests). Unions race-level records from each source into a
-- standardized schema for Splink matching.
--
-- Grain: One row per source race record (election stage)
-- Key: unique_id (source_name || '|' || source_id)
--
-- The matcha election_stage entity reads this view. Cluster output lands in
-- goodparty_data_catalog.er_source.er_clustered_election_stages and feeds
-- int__civics_er_canonical_election_stages in a follow-up PR.
--
-- Notes on per-source coverage:
-- - BR has native state (via br_position), is_special, ballotready_position_id.
-- - DDHQ has state_postal_code; is_special is derived from stage_type.
-- ballotready_position_id is NULL for V1 — DDHQ has no native position FK,
-- and the BR position resolution requires the election_stage ER clusters
-- that THIS view is itself seeding (chicken-and-egg). Revisit once
-- int__civics_er_canonical_election_stages exists.
-- - TS has no native state column; we extract the 2-letter prefix from
-- race_name (which TS constructs as `state || ' ' || official_office_name`).
-- is_special is always false (TS does not surface special-election flags).
-- - official_office_name strips the leading 2-letter state prefix on every
-- source. state is already a dedicated ExactMatch comparison, so keeping the
-- prefix inside the office string only inflates Jaro-Winkler on the shared
-- prefix and guarantees a shared locality token (defeating office
-- discrimination across distinct races in the same state).
-- - Office attributes (candidate_office, office_level, office_type,
-- district_raw, district_identifier, seat_name) are carried up from the
-- source election_stage models. district_identifier is parsed from the
-- district string. seat_name is BR-only; the others are populated on all
-- three sources.
with
    -- BallotReady positions provide state for BR election stages
    br_position as (
        select database_id as br_position_id, state
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    ballotready_stages as (
        select
            'ballotready' as source_name,
            cast(es.br_race_id as string) as source_id,
            cast(es.br_position_id as bigint) as ballotready_position_id,
            bp.state as state,
            nullif(
                regexp_replace(lower(trim(es.race_name)), '^[a-z]{2} ', ''), ''
            ) as official_office_name,
            es.candidate_office,
            es.office_level,
            es.office_type,
            nullif(es.district, '') as district_raw,
            try_cast(
                regexp_extract(es.district, '([0-9]+)') as int
            ) as district_identifier,
            nullif(es.seat_name, '') as seat_name,
            try_cast(es.br_race_id as int) as br_race_id_int,
            es.election_date,
            es.stage_type as election_stage,
            es.is_special,
            es.is_primary,
            es.is_runoff,
            es.number_of_seats
        from {{ ref("int__civics_election_stage_ballotready") }} as es
        left join br_position as bp on es.br_position_id = bp.br_position_id
        where bp.state is not null
    ),

    ddhq_stages as (
        select
            'ddhq' as source_name,
            -- gp_election_stage_id is 1:1 with rows in the DDHQ int model
            -- (it's the dedupe key). A given ddhq_race_id can map to >1 stage
            -- when DDHQ surfaces e.g. primary + general for the same race id,
            -- so we cannot use ddhq_race_id as the source PK without losing
            -- uniqueness in this view.
            cast(gp_election_stage_id as string) as source_id,
            -- V1: leave NULL; see header comment.
            cast(null as bigint) as ballotready_position_id,
            state_postal_code as state,
            nullif(
                regexp_replace(lower(trim(race_name)), '^[a-z]{2} ', ''), ''
            ) as official_office_name,
            candidate_office,
            office_level,
            office_type,
            nullif(district, '') as district_raw,
            coalesce(
                try_cast(regexp_extract(district, '([0-9]+)') as int),
                try_cast(regexp_extract(race_name, ' ([0-9]+)$') as int)
            ) as district_identifier,
            seat_name,
            cast(null as int) as br_race_id_int,
            election_date,
            stage_type as election_stage,
            stage_type like '%special%' as is_special,
            is_primary,
            is_runoff,
            cast(null as int) as number_of_seats
        from {{ ref("int__civics_election_stage_ddhq") }}
        where state_postal_code is not null
    ),

    techspeed_stages as (
        select
            'techspeed' as source_name,
            cast(gp_election_stage_id as string) as source_id,
            cast(null as bigint) as ballotready_position_id,
            -- TS race_name is `state || ' ' || official_office_name` (see
            -- int__civics_election_stage_techspeed); extract the prefix.
            substring(race_name, 1, 2) as state,
            -- Strip the leading 2-letter state prefix (see header note);
            -- applied uniformly across BR/DDHQ/TS so the office string carries
            -- only the locality + office, not the state.
            nullif(
                regexp_replace(lower(trim(race_name)), '^[a-z]{2} ', ''), ''
            ) as official_office_name,
            candidate_office,
            -- TS staging emits mixed-case office_level; initcap to match BR/DDHQ.
            initcap(office_level) as office_level,
            office_type,
            nullif(district, '') as district_raw,
            try_cast(
                regexp_extract(district, '([0-9]+)') as int
            ) as district_identifier,
            nullif(seat_name, '') as seat_name,
            cast(null as int) as br_race_id_int,
            election_date,
            stage_type as election_stage,
            false as is_special,
            is_primary,
            is_runoff,
            number_of_seats
        from {{ ref("int__civics_election_stage_techspeed") }}
        where race_name rlike '^[A-Z]{2} '
    ),

    unioned as (
        select *
        from ballotready_stages
        union all
        select *
        from ddhq_stages
        union all
        select *
        from techspeed_stages
    )

select
    source_name || '|' || source_id as unique_id,
    source_id,
    u.source_name,
    u.state,
    u.official_office_name,
    u.candidate_office,
    -- Race-level matcher has no person fields, but matcha's shared
    -- pipeline.load_and_prepare requires the column; emit an empty array.
    array() as first_name_aliases,
    u.election_date,
    u.election_stage,
    u.is_special,
    u.is_primary,
    u.is_runoff,
    u.number_of_seats,
    u.ballotready_position_id,
    u.br_race_id_int as br_race_id,
    -- Office attributes carried up from the source election_stage models.
    -- Sparse on some sources (e.g. seat_name is BR-only); Splink's NullLevel
    -- handles per-row missing values.
    u.office_level,
    u.office_type,
    u.district_identifier,
    u.district_raw,
    u.seat_name
from unioned as u
