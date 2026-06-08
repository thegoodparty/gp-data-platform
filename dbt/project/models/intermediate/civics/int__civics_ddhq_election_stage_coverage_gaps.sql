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
            es.stage_type,
            -- Partisan primaries are out of scope for the DDHQ coverage
            -- agreement (independent / 3rd-party candidates are ineligible), so
            -- flag them for exclusion from the agreement-scoped gap count.
            coalesce(
                es.partisan_type = 'partisan' and es.is_primary, false
            ) as is_partisan_primary
        from {{ ref("int__civics_election_stage_ballotready") }} as es
        left join br_position as bp on es.br_position_id = bp.br_position_id
        where
            es.election_date >= date '2026-01-01'
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

    -- BR candidacies per race (the candidate count on the BR side). Source from
    -- the race-grain S3 staging so every stage's br_race_id gets its own count;
    -- the rolled-up int__civics_candidacy_ballotready collapses all stages onto
    -- one any_value br_race_id, which would zero-out the non-selected stages.
    br_candidacy_counts as (
        select
            cast(br_race_id as string) as br_race_id,
            count(distinct cast(br_candidate_id as string)) as br_candidate_count
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        -- No election_day filter: the join to br_local (below) already scopes
        -- counts to date-bounded races via br_race_id (stage-specific, so it
        -- pins the date). An S3-side date filter is redundant and silently
        -- drops candidacies whose election_day the staging model NULLed out.
        where br_race_id is not null
        group by br_race_id
    ),

    -- Latest election DDHQ has loaded. Rows dated after this are beyond DDHQ's
    -- data horizon (not yet ingested), so they are not meaningful coverage gaps;
    -- the flag below marks rows on/before it. Computed dynamically so it tracks
    -- as DDHQ loads more.
    ddhq_horizon as (
        select max(election_date) as last_ddhq_reported_election_date
        from {{ ref("int__civics_election_stage_ddhq") }}
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
    b.is_partisan_primary,
    ts.canonical_gp_election_stage_id is not null as has_techspeed_match,
    coalesce(cc.br_candidate_count, 0) as br_candidate_count,
    h.last_ddhq_reported_election_date,
    -- coalesce to false when DDHQ has no data (empty source -> NULL horizon),
    -- so the flag is never NULL.
    coalesce(
        b.election_date <= h.last_ddhq_reported_election_date, false
    ) as is_before_last_ddhq_reported_election_date
from br_local as b
left join
    ddhq_matched as dq on b.gp_election_stage_id = dq.canonical_gp_election_stage_id
left join ts_matched as ts on b.gp_election_stage_id = ts.canonical_gp_election_stage_id
left join br_candidacy_counts as cc on b.br_race_id = cc.br_race_id
cross join ddhq_horizon as h
-- Null-safe anti-join: keep only BR races with NO DDHQ match.
where dq.canonical_gp_election_stage_id is null
