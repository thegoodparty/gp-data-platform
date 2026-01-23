{{
    config(
        materialized="table",
    )
}}

-- Civics mart candidacy_stage table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
-- Filters to only include candidacy_stages with valid candidacies and election_stages
-- for referential integrity
with
    valid_candidacies as (select gp_candidacy_id from {{ ref("candidacy") }}),

    valid_election_stages as (
        select gp_election_stage_id from {{ ref("election_stage") }}
    ),

    filtered_candidacy_stages as (
        select *
        from {{ ref("candidacy_stage_20260122") }}
        where
            gp_candidacy_id in (select gp_candidacy_id from valid_candidacies)
            and (
                gp_election_stage_id is null
                or gp_election_stage_id
                in (select gp_election_stage_id from valid_election_stages)
            )
    )

select
    gp_candidacy_stage_id,
    gp_candidacy_id,
    gp_election_stage_id,
    candidate_name,
    source_candidate_id,
    source_race_id,
    candidate_party,
    is_winner,
    election_result,
    election_result_source,
    match_confidence,
    match_reasoning,
    match_top_candidates,
    has_match,
    votes_received,
    election_stage_date,
    created_at,
    updated_at

from filtered_candidacy_stages
