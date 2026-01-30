{{
    config(
        materialized="table",
    )
}}

-- Civics mart candidacy_stage table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
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

from {{ ref("int__civics_candidacy_stage_2025") }}
