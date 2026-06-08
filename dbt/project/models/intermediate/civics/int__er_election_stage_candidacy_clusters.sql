-- Bridge: for each election_stage (at the int__er_prematch_election_stages
-- source_name + source_id grain), the set of candidacy_stage ER cluster_ids of
-- the candidacies belonging to it. Two election_stages that share a candidacy
-- cluster have a matched candidacy in common -- strong evidence they are the
-- same race even when their office names diverge across sources. The
-- election_stage matcher uses this as a blocking key + office-identity bypass
-- (mirrors the br_race_id anchor, but reaches DDHQ, which carries no
-- br_race_id and can only link to BR via candidate identity).
--
-- Scope: BallotReady + DDHQ. TechSpeed election_stages already anchor to BR via
-- br_race_id, so TS candidacy overlap is redundant and omitted here.
--
-- Grain: one row per (source_name, source_id) that has >= 1 matched candidacy.
with
    clustered as (select * from {{ ref("stg_er_source__clustered_candidacy_stages") }}),

    ddhq_stages as (
        select gp_election_stage_id, ddhq_race_id, stage_type
        from {{ ref("int__civics_election_stage_ddhq") }}
    ),

    -- BR: a BR candidacy's br_race_id IS its election_stage source_id (BR
    -- br_race_id is stage-grain, 1:1 with the BR election_stage prematch row).
    br_map as (
        select
            'ballotready' as source_name,
            cast(br_race_id as string) as source_id,
            cluster_id
        from clustered
        where source_name = 'ballotready' and br_race_id is not null
    ),

    -- DDHQ: the candidacy source_id is `candidate_id || '_' || ddhq_race_id`;
    -- recover ddhq_race_id and join the DDHQ election_stage model on
    -- (ddhq_race_id, stage) to get its gp_election_stage_id (the DDHQ
    -- election_stage prematch source_id). Joining on stage keeps a primary
    -- candidacy's cluster off the general stage's row.
    ddhq_map as (
        select
            'ddhq' as source_name,
            cast(es.gp_election_stage_id as string) as source_id,
            clustered.cluster_id
        from clustered
        join
            ddhq_stages as es
            on substring_index(clustered.source_id, '_', -1)
            = cast(es.ddhq_race_id as string)
            -- candidacy election_stage is Title Case ("General"); the DDHQ
            -- election_stage model emits lowercase stage_type ("general").
            and lower(clustered.election_stage) = es.stage_type
        where clustered.source_name = 'ddhq'
    ),

    combined as (
        select *
        from br_map
        union all
        select *
        from ddhq_map
    )

select
    source_name, source_id, collect_set(cluster_id) as matched_candidacy_stage_clusters
from combined
group by source_name, source_id
