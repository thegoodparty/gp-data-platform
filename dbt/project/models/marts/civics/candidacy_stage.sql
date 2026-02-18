-- Civics mart candidacy_stage table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
with
    combined as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            candidate_name,
            source_candidate_id,
            source_race_id,
            candidate_party,
            is_winner,
            -- Normalize election_result values from HubSpot
            case
                when election_result = 'Won General'
                then 'Won'
                when election_result = 'Lost General'
                then 'Lost'
                else election_result
            end as election_result,
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

        union all

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
        from {{ ref("int__civics_candidacy_stage_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc nulls last
            )
            = 1
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

from deduplicated
