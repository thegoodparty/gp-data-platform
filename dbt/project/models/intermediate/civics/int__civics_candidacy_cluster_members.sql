-- Candidacy-stage cluster members as person record keys (unique_id in
-- int__er_prematch_people): BallotReady via the candidacy's person,
-- TechSpeed at candidate-code grain, gp_api via the campaign's user. One row
-- per (cluster_id, record_key), restricted to the person universe so the
-- person links and the DDHQ person lookup read one set. A cluster spanning
-- two BallotReady people cannot name a person, so it contributes no members.
with
    prematch as (select unique_id from {{ ref("int__er_prematch_people") }}),

    candidacies as (
        select distinct
            cast(br_candidacy_id as string) as br_candidacy_id,
            cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null and br_candidate_id is not null
    ),

    campaigns as (
        select
            cast(campaign_id as string) as campaign_id,
            cast(user_id as string) as user_id
        from {{ ref("campaigns") }}
        where is_latest_version and user_id is not null
    ),

    clustered as (
        select
            cluster_id,
            source_name,
            source_id,
            cast(br_candidacy_id as string) as br_candidacy_id,
            split(source_id, '__')[0] as gp_api_campaign_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
    ),

    members as (
        select cc.cluster_id, 'ballotready|' || cand.br_candidate_id as record_key
        from clustered as cc
        inner join candidacies as cand using (br_candidacy_id)
        where cc.source_name = 'ballotready'
        union
        select cc.cluster_id, 'gp_api|' || camp.user_id
        from clustered as cc
        inner join campaigns as camp on camp.campaign_id = cc.gp_api_campaign_id
        where cc.source_name = 'gp_api'
        union
        select cluster_id, 'techspeed|' || {{ strip_ts_stage_suffix("source_id") }}
        from clustered
        where source_name = 'techspeed'
    ),

    resolvable as (
        select cluster_id
        from members
        group by cluster_id
        having
            count(
                distinct case when record_key like 'ballotready|%' then record_key end
            )
            <= 1
    )

select m.cluster_id, m.record_key
from members as m
inner join resolvable using (cluster_id)
inner join prematch as p on p.unique_id = m.record_key
