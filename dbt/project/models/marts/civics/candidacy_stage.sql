-- Civics mart candidacy_stage table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed data
--
-- The 2026+ portion uses cluster_id from entity resolution to match BR and TS
-- candidacy_stages, then applies survivorship rules (BR wins by default).
with
    -- =========================================================================
    -- 2025 archive (pass-through, no merge needed)
    -- =========================================================================
    archive_2025 as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
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

    -- =========================================================================
    -- Attach cluster_id to each source's candidacy_stages
    -- =========================================================================
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

    -- =========================================================================
    -- FK remaps: when a TS candidacy/election_stage is matched to BR via any
    -- cluster, remap the TS FK to the BR FK so all stages of a matched
    -- candidacy share the same gp_candidacy_id.
    -- =========================================================================
    candidacy_remap as (
        select distinct ts_m.gp_candidacy_id as ts_id, br_m.gp_candidacy_id as br_id
        from {{ ref("int__civics_cluster_members") }} as br_m
        inner join {{ ref("int__civics_cluster_members") }} as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    election_stage_remap as (
        select distinct
            ts_m.gp_election_stage_id as ts_id, br_m.gp_election_stage_id as br_id
        from {{ ref("int__civics_cluster_members") }} as br_m
        inner join {{ ref("int__civics_cluster_members") }} as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    -- =========================================================================
    -- Merge 2026+ BR + TS with survivorship (BR wins by default)
    -- =========================================================================
    merged_2026 as (
        select
            coalesce(
                br.gp_candidacy_stage_id, ts.gp_candidacy_stage_id
            ) as gp_candidacy_stage_id,
            coalesce(
                br.gp_candidacy_id, cr.br_id, ts.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                br.gp_election_stage_id, esr.br_id, ts.gp_election_stage_id
            ) as gp_election_stage_id,
            coalesce(br.candidate_name, ts.candidate_name) as candidate_name,
            coalesce(
                br.source_candidate_id, ts.source_candidate_id
            ) as source_candidate_id,
            coalesce(br.source_race_id, ts.source_race_id) as source_race_id,
            coalesce(br.candidate_party, ts.candidate_party) as candidate_party,
            coalesce(br.is_winner, ts.is_winner) as is_winner,
            coalesce(br.election_result, ts.election_result) as election_result,
            coalesce(
                br.election_result_source, ts.election_result_source
            ) as election_result_source,
            coalesce(br.match_confidence, ts.match_confidence) as match_confidence,
            coalesce(br.match_reasoning, ts.match_reasoning) as match_reasoning,
            coalesce(
                br.match_top_candidates, ts.match_top_candidates
            ) as match_top_candidates,
            coalesce(br.has_match, ts.has_match) as has_match,
            coalesce(br.votes_received, ts.votes_received) as votes_received,
            coalesce(
                br.election_stage_date, ts.election_stage_date
            ) as election_stage_date,
            coalesce(br.created_at, ts.created_at) as created_at,
            coalesce(br.updated_at, ts.updated_at) as updated_at,
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

    -- =========================================================================
    -- Combine archive + merged 2026+
    -- =========================================================================
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
