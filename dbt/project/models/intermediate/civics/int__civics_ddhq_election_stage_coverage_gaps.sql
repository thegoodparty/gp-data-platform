{{ config(materialized="view", tags=["civics", "entity_resolution"]) }}

-- Diagnostic: BallotReady local election_stages (election_date >= 2026-01-01)
-- with NO matched DDHQ counterpart, per the entity-resolution crosswalk
-- int__civics_er_canonical_election_stages. Used to triage gaps in DDHQ
-- election coverage. Grain: one row per BR gp_election_stage_id with no DDHQ
-- match.
--
-- NOTE: in dev the crosswalk reflects the OR-baseline matcher (no anchors), so
-- dev counts overstate the gap; self-corrects once prod's crosswalk is rebuilt
-- from the final-config matcher. See the design doc.
with
    br_position as (
        select database_id as br_position_id, state
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    -- In-scope BR races: local (non-state/non-federal) stages on/after 2026-01-01.
    br_local as (
        select
            es.gp_election_stage_id,
            es.br_race_id,
            bp.state,
            {{ strip_office_name_state_prefix("es.race_name") }}
            as official_office_name,
            es.candidate_office,
            es.office_level,
            es.office_type,
            try_cast(
                regexp_extract(es.district, '([0-9]+)') as int
            ) as district_identifier,
            es.seat_name,
            es.election_date,
            es.stage_type
        from {{ ref("int__civics_election_stage_ballotready") }} as es
        left join br_position as bp on es.br_position_id = bp.br_position_id
        where
            es.election_date >= '2026-01-01'
            and es.office_level not in ('State', 'Federal')
    ),

    -- BR races that DID get a DDHQ / TS match: their gp_election_stage_id is the
    -- canonical of a DDHQ / TS crosswalk row (BR-anchored clusters).
    ddhq_matched as (
        select distinct canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_election_stages") }}
        where source_name = 'ddhq' and canonical_gp_election_stage_id is not null
    ),

    ts_matched as (
        select distinct canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_election_stages") }}
        where source_name = 'techspeed' and canonical_gp_election_stage_id is not null
    ),

    -- BR candidacies per race (the candidate count on the BR side).
    br_candidacy_counts as (
        select br_race_id, count(distinct gp_candidacy_id) as br_candidate_count
        from {{ ref("int__civics_candidacy_ballotready") }}
        where br_race_id is not null
        group by br_race_id
    )

select
    b.gp_election_stage_id,
    b.br_race_id,
    b.state,
    b.official_office_name,
    b.candidate_office,
    b.office_level,
    b.office_type,
    b.district_identifier,
    b.seat_name,
    b.election_date,
    b.stage_type,
    ts.canonical_gp_election_stage_id is not null as has_techspeed_match,
    coalesce(cc.br_candidate_count, 0) as br_candidate_count
from br_local as b
left join
    ddhq_matched as dq on b.gp_election_stage_id = dq.canonical_gp_election_stage_id
left join ts_matched as ts on b.gp_election_stage_id = ts.canonical_gp_election_stage_id
left join br_candidacy_counts as cc on b.br_race_id = cc.br_race_id
-- Null-safe anti-join: keep only BR races with NO DDHQ match.
where dq.canonical_gp_election_stage_id is null
