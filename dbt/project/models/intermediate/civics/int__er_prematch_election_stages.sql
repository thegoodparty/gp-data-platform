{{ config(materialized="view") }}

-- Entity Resolution prematch: BallotReady x DDHQ x TechSpeed election stages
-- (races/contests). Unions race-level records from each source into a
-- standardized schema for Splink matching.
--
-- Grain: One row per source race record (election stage)
-- Key: unique_id (source_name || '|' || source_id)
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
            {{ strip_office_name_state_prefix("es.race_name") }}
            as official_office_name,
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
            -- V1: leave NULL; DDHQ has no native position FK.
            cast(null as bigint) as ballotready_position_id,
            state_postal_code as state,
            {{ strip_office_name_state_prefix("race_name") }} as official_office_name,
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
            {{ strip_office_name_state_prefix("race_name") }} as official_office_name,
            candidate_office,
            -- TS staging emits mixed-case office_level; initcap to match BR/DDHQ.
            initcap(office_level) as office_level,
            office_type,
            nullif(district, '') as district_raw,
            try_cast(
                regexp_extract(district, '([0-9]+)') as int
            ) as district_identifier,
            nullif(seat_name, '') as seat_name,
            -- TS rows carry a BallotReady br_race_id (the source's own reference
            -- to a BR race). Surface it so the matcher can block on br_race_id
            -- and anchor TS to its BR race, recovering matches the office/geo
            -- blocking rules miss. ~91% of distinct TS br_race_ids point to a
            -- real BR race; the post-prediction filter still confirms the pair.
            try_cast(br_race_id as int) as br_race_id_int,
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
    ),

    -- Only BallotReady carries a seat_name column, and it covers the "seat N"
    -- naming but not the "(surname seat)" one, so the signal was unusable across
    -- sources: every distinct seat of a multi-seat body looked identical, and a
    -- single NULL-seat record hub-chained them into one cluster (307 LA County
    -- judicial races merged into one). All three sources spell the seat inside
    -- official_office_name in the same vocabulary, so parse it uniformly.
    -- "at large" is deliberately not parsed: it marks the absence of a district
    -- and identifies no particular seat.
    seat_resolved as (
        select
            *,
            lower(
                trim(
                    coalesce(
                        seat_name,
                        nullif(
                            regexp_extract(
                                official_office_name, '\\(([^)]+) seat\\)', 1
                            ),
                            ''
                        ),
                        nullif(
                            regexp_extract(
                                official_office_name, 'seat ([0-9]+|[a-z])\\b', 1
                            ),
                            ''
                        ),
                        nullif(
                            regexp_extract(
                                official_office_name,
                                '(?:position|office) (?:no\\.? )?([0-9]+)',
                                1
                            ),
                            ''
                        ),
                        -- group/place/division are the same idea under other
                        -- local names (FL circuit courts, TX councils). Letters
                        -- are allowed only as a single character so "division of
                        -- elections" cannot parse to "of".
                        nullif(
                            regexp_extract(official_office_name, 'group ([0-9]+)', 1),
                            ''
                        ),
                        nullif(
                            regexp_extract(official_office_name, 'place ([0-9]+)', 1),
                            ''
                        ),
                        nullif(
                            regexp_extract(
                                official_office_name, 'division ([0-9]+|[a-z])\\b', 1
                            ),
                            ''
                        )
                    )
                )
            ) as seat_lowered
        from unioned
    )

select
    u.source_name || '|' || u.source_id as unique_id,
    u.source_id,
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
    -- Sparse on some sources; Splink's NullLevel handles per-row missing values.
    u.office_level,
    u.office_type,
    u.district_identifier,
    u.district_raw,
    -- De-zero-padded so a parsed "01" matches BallotReady's "1"; without it the
    -- matcher's seat clause would fail closed on values that agree.
    case
        when u.seat_lowered rlike '^[0-9]+$'
        then cast(cast(u.seat_lowered as int) as string)
        else u.seat_lowered
    end as seat_name,
    -- Candidacy-overlap signal: the candidacy_stage ER cluster_ids of this
    -- race's candidacies. Two election_stages sharing a cluster have a matched
    -- candidacy in common (same race even when office names diverge). The
    -- matcher blocks + bypasses office identity on this; empty for races with
    -- no matched candidacies (e.g. upcoming elections), which fall back to the
    -- office/geo path. BR + DDHQ only (TS already anchors via br_race_id).
    coalesce(
        cc.matched_candidacy_stage_clusters, array()
    ) as matched_candidacy_stage_clusters
from seat_resolved as u
left join
    {{ ref("int__er_election_stage_candidacy_clusters") }} as cc
    on u.source_name = cc.source_name
    and u.source_id = cc.source_id
