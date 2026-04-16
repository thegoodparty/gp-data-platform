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

    -- Attach cluster_id for the full outer join
    br_with_cluster as (
        select cm.cluster_id, br.*
        from {{ ref("int__civics_candidacy_stage_ballotready") }} as br
        left join
            {{ ref("int__civics_cluster_members") }} as cm
            on br.gp_candidacy_stage_id = cm.gp_candidacy_stage_id
            and cm.source_name = 'ballotready'
    ),

    ts_with_cluster as (
        select cm.cluster_id, ts.*
        from {{ ref("int__civics_candidacy_stage_techspeed") }} as ts
        left join
            {{ ref("int__civics_cluster_members") }} as cm
            on ts.gp_candidacy_stage_id = cm.gp_candidacy_stage_id
            and cm.source_name = 'techspeed'
    ),

    -- FK remaps applied to ALL TS records (not just those in cluster_members).
    -- Derived from pre-computed remaps — no self-join needed.
    candidacy_remap as (
        select distinct gp_candidacy_id as ts_id, remap_candidacy_id as br_id
        from {{ ref("int__civics_cluster_members") }}
        where source_name = 'techspeed' and remap_candidacy_id is not null
    ),

    election_stage_remap as (
        select distinct gp_election_stage_id as ts_id, remap_election_stage_id as br_id
        from {{ ref("int__civics_cluster_members") }}
        where source_name = 'techspeed' and remap_election_stage_id is not null
    ),

    -- BR wins by default for all columns
    merged_2026 as (
        select
            coalesce(
                br.gp_candidacy_stage_id, ts.gp_candidacy_stage_id
            ) as gp_candidacy_stage_id,
            -- FK remaps: BR's ID > cross-cluster BR match > TS's own
            coalesce(
                br.gp_candidacy_id, cr.br_id, ts.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                br.gp_election_stage_id, esr.br_id, ts.gp_election_stage_id
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
        from br_with_cluster as br
        full outer join ts_with_cluster as ts on br.cluster_id = ts.cluster_id
        left join candidacy_remap as cr on ts.gp_candidacy_id = cr.ts_id
        left join election_stage_remap as esr on ts.gp_election_stage_id = esr.ts_id
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
