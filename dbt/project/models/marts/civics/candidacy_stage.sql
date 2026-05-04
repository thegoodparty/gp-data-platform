-- Civics mart candidacy_stage table.
-- 2025 HubSpot archive UNION 2026+ 4-way cluster-based FOJ over BR + TS +
-- DDHQ + gp_api. Each provider is wrapped with its Splink cluster_id from
-- stg_er_source__clustered_candidacy_stages; the FOJ joins on
-- coalesce(cluster_id, 'self_' || gp_candidacy_stage_id), so any provider
-- combination collapses to one mart row.
-- Per-column precedence rules: see the candidacy_stage model description
-- in m_civics.yaml.
{%- set gp_api_wins_cols = [
    "candidate_name",
    "source_candidate_id",
    "candidate_party",
    "election_stage_date",
    "created_at",
    "updated_at",
] %}
{%- set ddhq_wins_cols = [
    "is_winner",
    "election_result",
    "election_result_source",
    "votes_received",
] %}

with
    archive_2025 as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            -- Column order must match merged_since_2026 (gp_api_wins_cols,
            -- source_race_id, ddhq_wins_cols, BR/TS-only match_*,
            -- is_uncontested, source_systems, diagnostics)
            candidate_name,
            cast(source_candidate_id as string) as source_candidate_id,
            candidate_party,
            election_stage_date,
            created_at,
            updated_at,
            cast(source_race_id as string) as source_race_id,
            is_winner,
            election_result,
            election_result_source,
            votes_received,
            cast(match_confidence as float) as match_confidence,
            match_reasoning,
            match_top_candidates,
            has_match,
            cast(null as boolean) as is_uncontested,
            array_compact(
                array('hubspot', case when has_match then 'ddhq' end)
            ) as source_systems,
            cast(null as string) as er_cluster_id,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as ts_source_candidate_id,
            cast(null as string) as gp_api_campaign_id,
            -- Archive's source_candidate_id / source_race_id ARE the DDHQ
            -- keys when has_match=true.
            case
                when has_match then cast(source_candidate_id as string)
            end as ddhq_candidate_id,
            case when has_match then cast(source_race_id as string) end as ddhq_race_id
        from {{ ref("int__civics_candidacy_stage_2025") }}
    ),

    -- DDHQ projected into the BR/TS merge-row schema (column rename only).
    ddhq as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            candidate_full_name as candidate_name,
            source_candidate_id,
            source_race_id,
            party_affiliation as candidate_party,
            election_date as election_stage_date,
            created_at,
            updated_at,
            is_winner,
            election_result,
            election_result_source,
            cast(votes as string) as votes_received,
            is_uncontested
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
    ),

    -- Each provider wrapped with its Splink cluster_id and a merge_key.
    br_with_cluster as (
        select
            br.*,
            cw.cluster_id,
            coalesce(cw.cluster_id, 'self_' || br.gp_candidacy_stage_id) as merge_key
        from {{ ref("int__civics_candidacy_stage_ballotready") }} as br
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'ballotready'
            and cw.br_candidacy_id = br.br_candidacy_id
    ),

    ts_with_cluster as (
        select
            ts.*,
            cw.cluster_id,
            coalesce(cw.cluster_id, 'self_' || ts.gp_candidacy_stage_id) as merge_key
        from {{ ref("int__civics_candidacy_stage_techspeed") }} as ts
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'techspeed'
            and regexp_replace(cw.source_id, '__(primary|general|runoff)$', '')
            = ts.source_candidate_id
            and cw.election_date = ts.election_stage_date
    ),

    ddhq_with_cluster as (
        select
            ddhq.*,
            cw.cluster_id,
            coalesce(cw.cluster_id, 'self_' || ddhq.gp_candidacy_stage_id) as merge_key
        from ddhq
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'ddhq'
            and cw.source_id = ddhq.source_candidate_id || '_' || ddhq.source_race_id
    ),

    -- gp_api: cluster source_id is '{campaign_id}__{stage}'. The int model's
    -- source_candidate_id is cast(campaign_id as string), and election_stage_date
    -- equals cw.election_date when clustered. Mirrors the lookup in
    -- int__civics_er_canonical_ids' gp_api_stage_matches branch.
    gp_api_with_cluster as (
        select
            gp_api.*,
            cw.cluster_id,
            coalesce(
                cw.cluster_id, 'self_' || gp_api.gp_candidacy_stage_id
            ) as merge_key
        from {{ ref("int__civics_candidacy_stage_gp_api") }} as gp_api
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'gp_api'
            and split(cw.source_id, '__')[0] = gp_api.source_candidate_id
            and cw.election_date = gp_api.election_stage_date
    ),

    merged_since_2026 as (
        select
            coalesce(
                gp_api.gp_candidacy_stage_id,
                br.gp_candidacy_stage_id,
                ts.gp_candidacy_stage_id,
                ddhq.gp_candidacy_stage_id
            ) as gp_candidacy_stage_id,
            coalesce(
                gp_api.gp_candidacy_id,
                br.gp_candidacy_id,
                ts.gp_candidacy_id,
                ddhq.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                gp_api.gp_election_stage_id,
                br.gp_election_stage_id,
                ts.gp_election_stage_id,
                ddhq.gp_election_stage_id
            ) as gp_election_stage_id,
            -- gp_api > BR > TS > DDHQ for descriptive cols
            {% for col in gp_api_wins_cols %}
                coalesce(
                    gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}
                ) as {{ col }},
            {% endfor %}
            -- source_race_id: BR > TS > DDHQ; gp_api carries no value here.
            coalesce(
                br.source_race_id, ts.source_race_id, ddhq.source_race_id
            ) as source_race_id,
            -- DDHQ wins for results columns (gp_api carries no values)
            {% for col in ddhq_wins_cols %}
                coalesce(ddhq.{{ col }}, br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            -- LLM-match metadata only exists on BR/TS
            coalesce(br.match_confidence, ts.match_confidence) as match_confidence,
            coalesce(br.match_reasoning, ts.match_reasoning) as match_reasoning,
            coalesce(
                br.match_top_candidates, ts.match_top_candidates
            ) as match_top_candidates,
            coalesce(br.has_match, ts.has_match) as has_match,
            -- is_uncontested only exists on DDHQ at this grain
            ddhq.is_uncontested,
            array_compact(
                array(
                    case when br.merge_key is not null then 'ballotready' end,
                    case when ts.merge_key is not null then 'techspeed' end,
                    case when ddhq.merge_key is not null then 'ddhq' end,
                    case when gp_api.merge_key is not null then 'gp_api' end
                )
            ) as source_systems,
            coalesce(
                br.cluster_id, ts.cluster_id, ddhq.cluster_id, gp_api.cluster_id
            ) as er_cluster_id,
            br.br_candidacy_id,
            ts.source_candidate_id as ts_source_candidate_id,
            gp_api.source_candidate_id as gp_api_campaign_id,
            ddhq.source_candidate_id as ddhq_candidate_id,
            ddhq.source_race_id as ddhq_race_id
        from br_with_cluster as br
        full outer join ts_with_cluster as ts on br.merge_key = ts.merge_key
        full outer join
            ddhq_with_cluster as ddhq
            on coalesce(br.merge_key, ts.merge_key) = ddhq.merge_key
        full outer join
            gp_api_with_cluster as gp_api
            on coalesce(br.merge_key, ts.merge_key, ddhq.merge_key) = gp_api.merge_key
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_since_2026
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
    deduplicated.is_uncontested,
    deduplicated.election_stage_date,
    es.is_win_icp,
    es.is_serve_icp,
    es.is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.er_cluster_id,
    deduplicated.br_candidacy_id,
    deduplicated.ts_source_candidate_id,
    deduplicated.gp_api_campaign_id,
    deduplicated.ddhq_candidate_id,
    deduplicated.ddhq_race_id,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("election_stage") }} as es
    on deduplicated.gp_election_stage_id = es.gp_election_stage_id
