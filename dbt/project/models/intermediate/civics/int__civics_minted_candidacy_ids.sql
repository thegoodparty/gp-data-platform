-- Minted gp_candidacy_id per candidacy-stage record. One row per clustered
-- candidacy-stage unique_id. Each candidacy-stage cluster is single-stage
-- (election dates may differ within matcha's 10-day window; distinct stages
-- sit >= 3 weeks apart), so its earliest-member mint is stage-grain.
-- gp_candidacy_id is candidacy-grain, so the consuming provider models roll this
-- up (min over their candidacy grouping); co-clustered candidacies converge
-- because they share the same clusters. Records in no cluster mint from
-- themselves via the provider self-mint fallback. See decision 6.
with
    -- Keyed first_seen fallbacks for cluster rows with no prematch row (stale
    -- ER assignments, stricter prematch filters), so a stale member cannot
    -- lose the mint to a later co-member via nulls-last. BR and gp_api carry
    -- native created timestamps keyed by the cluster source_id; TS/DDHQ fall
    -- back to the source's first-load time. Mirrors the person mint's chain.
    br_created as (
        select
            cast(br_candidacy_id as string) as source_id,
            min(candidacy_created_at) as first_seen_at
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null
        group by 1
    ),

    gp_api_created as (
        select
            cast(campaign_id as string) as source_id, min(created_at) as first_seen_at
        from {{ ref("campaigns") }}
        group by 1
    ),

    -- least() pins the first-load anchor: both connectors re-sync in overwrite
    -- mode, so a bare min(_airbyte_extracted_at) advances over time and could
    -- flip a mixed cluster's minting member. Pinned to the earliest observed
    -- load per source.
    ts_load_date as (
        select
            least(
                min(_airbyte_extracted_at), timestamp '2025-11-12 00:00:00'
            ) as first_seen_at
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
    ),

    ddhq_load_date as (
        select
            least(
                min(_airbyte_extracted_at), timestamp '2026-07-02 00:00:00'
            ) as first_seen_at
        from {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
    ),

    members as (
        select
            cl.cluster_id,
            cl.unique_id,
            cl.source_name,
            cl.source_id,
            coalesce(
                pm.first_seen_at,
                br.first_seen_at,
                gp.first_seen_at,
                case
                    when cl.source_name = 'techspeed'
                    then ts.first_seen_at
                    when cl.source_name = 'ddhq'
                    then dd.first_seen_at
                end
            ) as first_seen_at
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cl
        left join {{ ref("int__er_prematch_candidacy_stages") }} as pm using (unique_id)
        left join
            br_created as br
            on cl.source_name = 'ballotready'
            and cl.source_id = br.source_id
        left join
            gp_api_created as gp
            on cl.source_name = 'gp_api'
            and split(cl.source_id, '__')[0] = gp.source_id
        cross join ts_load_date as ts
        cross join ddhq_load_date as dd
    ),

    minting_member as (
        select
            cluster_id,
            source_name as minting_source_name,
            source_id as minting_source_id
        from members
        qualify
            row_number() over (
                partition by cluster_id
                order by first_seen_at asc nulls last, source_name asc, source_id asc
            )
            = 1
    )

select
    m.unique_id,
    m.source_name,
    m.source_id,
    m.cluster_id,
    m.first_seen_at,
    -- BR consumers only adopt the mint from sole-BR-member clusters:
    -- matcher-merged duplicate BR records keep distinct published ids
    -- (row-preserving), while vendor co-members still adopt one BR id via
    -- the crosswalk.
    count_if(m.source_name = 'ballotready') over (
        partition by m.cluster_id
    ) as cluster_br_members,
    {{
        generate_salted_uuid(
            fields=["mm.minting_source_name", "mm.minting_source_id"],
            salt="candidacy",
        )
    }} as minted_gp_candidacy_id
from members as m
inner join minting_member as mm using (cluster_id)
