{{
    config(
        materialized="table",
        tags=["mart", "civics", "historical"],
    )
}}

with
    archived_candidacy_stages as (
        -- Historical archive: election stages on or before 2025-12-31
        select *
        from {{ ref("m_general__candidacy_stage") }}
        where ddhq_election_stage_date <= '2025-12-31'
    ),

    -- Only include candidacy_stages that have a matching candidacy in the archive
    valid_candidacies as (select gp_candidacy_id from {{ ref("candidacy") }}),

    -- Only include candidacy_stages that have a matching election_stage in the archive
    valid_election_stages as (
        select gp_election_stage_id from {{ ref("election_stage") }}
    )

select
    stage.gp_candidacy_stage_id,
    stage.gp_candidacy_id,
    stage.gp_election_stage_id,
    stage.ddhq_candidate,
    stage.ddhq_candidate_id,
    stage.ddhq_race_id,
    stage.ddhq_candidate_party,
    stage.ddhq_is_winner,
    stage.ddhq_llm_confidence,
    stage.ddhq_llm_reasoning,
    stage.ddhq_top_10_candidates,
    stage.ddhq_has_match,
    stage.votes_received,
    stage.ddhq_election_stage_date,
    stage.created_at,
    stage.updated_at

from archived_candidacy_stages as stage
inner join
    valid_candidacies as candidacy on stage.gp_candidacy_id = candidacy.gp_candidacy_id
-- Filter to only include records with valid election_stage references (or null)
where
    stage.gp_election_stage_id is null
    or stage.gp_election_stage_id
    in (select gp_election_stage_id from valid_election_stages)
