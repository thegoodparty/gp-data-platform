-- Minted gp_election_stage_id per election-stage record. One row per clustered
-- election-stage unique_id. The id is minted from the cluster member earliest by
-- first_seen_at (ties: source_name, source_id) and shared by all members, so it
-- is stable under cluster churn. Records in no cluster mint from themselves via
-- the consuming election_stage models' self-mint fallback.
--
-- first_seen_at reads staging only (never the election-stage prematch or the
-- feeders): the BR feeder consumes this mint, so a prematch/feeder join here
-- would be a dbt cycle. BR races carry a native created timestamp keyed by
-- br_race_id (a race missing from staging ranks nulls-last); TS/DDHQ
-- election-stage keys are feeder-derived hashes, so their first_seen_at is the
-- source's pinned first-load time.
--
-- Known one-time churn: the live clustered table's TS/DDHQ source_ids are the
-- feeders' pre-swap hash-derived ids. The next election-stage matcha run
-- (phase 2) rewrites them, so vendor-minted clusters re-mint once at that
-- transition. Expected relabel, not a regression.
with
    br_race_created as (
        select cast(database_id as string) as br_race_id, created_at
        from {{ ref("stg_airbyte_source__ballotready_api_race") }}
    ),

    -- least() pins the first-load anchor: both connectors re-sync in overwrite
    -- mode, so a bare min(_airbyte_extracted_at) advances over time and could
    -- flip a mixed cluster's minting member when the BR race's created_at
    -- falls between loads. Pinned to the earliest observed load per source.
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
            case
                when cl.source_name = 'ballotready'
                then br.created_at
                when cl.source_name = 'techspeed'
                then ts.first_seen_at
                when cl.source_name = 'ddhq'
                then dd.first_seen_at
            end as first_seen_at
        from {{ ref("stg_er_source__clustered_election_stages") }} as cl
        left join br_race_created as br on cl.source_id = br.br_race_id
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
            salt="election_stage",
        )
    }} as minted_gp_election_stage_id
from members as m
inner join minting_member as mm using (cluster_id)
