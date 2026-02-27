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
    deduplicated.gp_candidacy_stage_id,
    deduplicated.gp_candidacy_id,
    deduplicated.gp_election_stage_id,
    deduplicated.candidate_name,
    deduplicated.source_candidate_id,
    deduplicated.source_race_id,
    deduplicated.candidate_party,
    deduplicated.is_winner,
    deduplicated.election_result,
    deduplicated.election_result_source,
    deduplicated.match_confidence,
    deduplicated.match_reasoning,
    deduplicated.match_top_candidates,
    deduplicated.has_match,
    deduplicated.votes_received,
    deduplicated.election_stage_date,
    es.is_win_icp,
    es.is_serve_icp,
    es.is_win_supersize_icp,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("election_stage") }} as es
    on deduplicated.gp_election_stage_id = es.gp_election_stage_id
