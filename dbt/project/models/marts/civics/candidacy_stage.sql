-- Civics mart candidacy_stage table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data.
-- Merge uses cluster_id from entity resolution; pre-computed remaps on
-- cluster_members ensure consistent FKs across stages of matched candidacies.
{%- set br_wins_cols = [
    "candidate_name",
    "source_candidate_id",
    "source_race_id",
    "candidate_party",
    "is_winner",
    "election_result",
    "election_result_source",
    "match_confidence",
    "match_reasoning",
    "match_top_candidates",
    "has_match",
    "votes_received",
    "election_stage_date",
    "created_at",
    "updated_at",
] %}

with
    archive_2025 as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            -- Column order must match merged_2026 (br_wins_cols loop order)
            candidate_name,
            cast(source_candidate_id as string) as source_candidate_id,
            cast(source_race_id as string) as source_race_id,
            candidate_party,
            is_winner,
            election_result,
            election_result_source,
            cast(match_confidence as float) as match_confidence,
            match_reasoning,
            match_top_candidates,
            has_match,
            votes_received,
            election_stage_date,
            created_at,
            updated_at,
            array('hubspot') as source_systems
        from {{ ref("int__civics_candidacy_stage_2025") }}
    ),

    -- TS int model remaps clustered rows to BR's gp_* IDs (via
    -- int__civics_er_canonical_ids), so a full outer join on gp_candidacy_stage_id
    -- merges matched pairs automatically.
    merged_2026 as (
        select
            coalesce(
                br.gp_candidacy_stage_id, ts.gp_candidacy_stage_id
            ) as gp_candidacy_stage_id,
            coalesce(br.gp_candidacy_id, ts.gp_candidacy_id) as gp_candidacy_id,
            coalesce(
                br.gp_election_stage_id, ts.gp_election_stage_id
            ) as gp_election_stage_id,
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            array_compact(
                array(
                    case
                        when br.gp_candidacy_stage_id is not null then 'ballotready'
                    end,
                    case when ts.gp_candidacy_stage_id is not null then 'techspeed' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidacy_stage_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidacy_stage_techspeed") }} as ts
            on br.gp_candidacy_stage_id = ts.gp_candidacy_stage_id
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_2026
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
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("election_stage") }} as es
    on deduplicated.gp_election_stage_id = es.gp_election_stage_id
