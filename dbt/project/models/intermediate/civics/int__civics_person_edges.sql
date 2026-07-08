-- Deterministic person edges. One row per typed edge between two record keys
-- (record_key = source_name || '|' || source_id). Edges are direction-agnostic
-- (record_key_1 <= record_key_2). No new matching: every edge derives from
-- native ids, candidacy-stage Splink cluster co-membership, or the
-- elected-official bridge.
with
    -- br_candidacy_id -> br_candidate_id (person grain).
    candidacies as (
        select distinct
            cast(br_candidacy_id as string) as br_candidacy_id,
            cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidacy_id is not null and br_candidate_id is not null
    ),

    users as (
        select id, hubspot_contact_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    contacts as (
        select
            id,
            cast(id as string) as id_string,
            goodparty_user_id,
            cast(br_candidacy_id as string) as br_candidacy_id
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    campaigns as (
        select cast(campaign_id as string) as campaign_id, user_id
        from {{ ref("campaigns") }}
        where is_latest_version and user_id is not null
    ),

    clustered as (
        select
            cluster_id,
            source_id,
            source_name,
            br_candidacy_id,
            split(source_id, '__')[0] as gp_api_campaign_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
    ),

    -- gp_api user <-> BR person via the elected-official bridge (E6 and the
    -- E7 conflict pre-filter both consume this).
    bridge as (
        select distinct
            gp_api_user_id, cast(br_candidate_id as string) as br_candidate_id
        from {{ ref("int__civics_elected_official_gp_api_bridge") }}
        where gp_api_user_id is not null and br_candidate_id is not null
    ),

    -- E1/E2: HubSpot contact <-> gp_api user via bidirectional native ids.
    -- Both directions collapse to one pair after normalization.
    e1 as (
        select
            'hubspot|' || cast(c.id as string) as rk_a,
            'gp_api|' || cast(c.goodparty_user_id as string) as rk_b
        from contacts as c
        inner join users as u on u.id = c.goodparty_user_id
        union
        select
            'gp_api|' || cast(u.id as string),
            'hubspot|' || cast(u.hubspot_contact_id as string)
        from users as u
        inner join contacts as c on c.id_string = u.hubspot_contact_id
    ),

    -- E3: HubSpot contact br_candidacy_id -> BR candidacy -> br_candidate_id.
    e3 as (
        select
            'hubspot|' || c.id_string as rk_a,
            'ballotready|' || cand.br_candidate_id as rk_b
        from contacts as c
        inner join candidacies as cand on cand.br_candidacy_id = c.br_candidacy_id
    ),

    -- E4: ts_officeholder_id == br_office_holder_id -> br_candidate_id.
    -- Reused ts_officeholder_ids suppressed (they conflate distinct people).
    e4 as (
        select
            'techspeed_officeholder|' || cast(ts_officeholder_id as string) as rk_a,
            'ballotready|' || cast(br_candidate_id as string) as rk_b
        from {{ ref("int__civics_elected_official_canonical_ids") }}
        where not ts_officeholder_id_is_reused and br_candidate_id is not null
    ),

    -- E5: candidacy-stage cluster co-membership. Map each member to its record
    -- key, then hub every member to the cluster's min record key (avoids a
    -- pairwise cross join). BR members map via candidacy; gp_api members map
    -- campaign -> user; TS/DDHQ members are their own keys.
    cluster_members as (
        select cc.cluster_id, 'ballotready|' || cand.br_candidate_id as record_key
        from clustered as cc
        inner join candidacies as cand on cand.br_candidacy_id = cc.br_candidacy_id
        where cc.source_name = 'ballotready'
        union
        select cc.cluster_id, 'gp_api|' || cast(camp.user_id as string)
        from clustered as cc
        inner join campaigns as camp on camp.campaign_id = cc.gp_api_campaign_id
        where cc.source_name = 'gp_api'
        union
        select cluster_id, 'techspeed|' || source_id
        from clustered
        where source_name = 'techspeed'
        union
        select cluster_id, 'ddhq|' || source_id
        from clustered
        where source_name = 'ddhq'
    ),

    cluster_hub as (
        select cluster_id, min(record_key) as hub_key
        from cluster_members
        group by cluster_id
    ),

    e5 as (
        select cm.record_key as rk_a, h.hub_key as rk_b
        from cluster_members as cm
        inner join cluster_hub as h using (cluster_id)
        where cm.record_key <> h.hub_key
    ),

    -- E6: elected-official bridge. gp_api user <-> BR person.
    e6 as (
        select
            'gp_api|' || cast(gp_api_user_id as string) as rk_a,
            'ballotready|' || br_candidate_id as rk_b
        from bridge
    ),

    -- E7: within-source vendor keys. TS records sharing a stage-stripped
    -- candidate_code; DDHQ records sharing candidate_id. Guards a vendor-only
    -- person's primary/general split. DDHQ candidate_id is reused across
    -- people ~1.5% of the time, so pre-filter: if a key's records already
    -- resolve (via clusters) to >1 distinct br_candidate_id, its E7 edges are
    -- flagged is_conflict and excluded from propagation downstream.
    e7_members as (
        select
            'techspeed' as source_name,
            source_id,
            {{ strip_ts_stage_suffix("source_id") }} as e7_key
        from clustered
        where source_name = 'techspeed'
        union all
        select 'ddhq', source_id, split(source_id, '_')[0]
        from clustered
        where source_name = 'ddhq'
    ),

    -- Distinct br_candidate_ids each vendor record reaches through its
    -- cluster: directly via a BR co-member, or via a gp_api co-member that
    -- resolves to a BR person through the elected-official bridge.
    vendor_cluster_br as (
        select cc.source_name, cc.source_id, cand.br_candidate_id
        from clustered as cc
        inner join
            clustered as br
            on br.cluster_id = cc.cluster_id
            and br.source_name = 'ballotready'
        inner join candidacies as cand on cand.br_candidacy_id = br.br_candidacy_id
        where cc.source_name in ('techspeed', 'ddhq')
        union
        select cc.source_name, cc.source_id, b.br_candidate_id
        from clustered as cc
        inner join
            clustered as gp
            on gp.cluster_id = cc.cluster_id
            and gp.source_name = 'gp_api'
        inner join campaigns as camp on camp.campaign_id = gp.gp_api_campaign_id
        inner join bridge as b on b.gp_api_user_id = camp.user_id
        where cc.source_name in ('techspeed', 'ddhq')
    ),

    e7_key_stats as (
        select
            m.source_name,
            m.e7_key,
            count(distinct v.br_candidate_id) as distinct_br,
            count(distinct m.source_id) as distinct_records
        from e7_members as m
        left join
            vendor_cluster_br as v
            on v.source_name = m.source_name
            and v.source_id = m.source_id
        group by m.source_name, m.e7_key
    ),

    e7_hub as (
        select source_name, e7_key, min(source_name || '|' || source_id) as hub_key
        from e7_members
        group by source_name, e7_key
    ),

    e7 as (
        select
            m.source_name || '|' || m.source_id as rk_a,
            h.hub_key as rk_b,
            s.distinct_br > 1 as is_conflict
        from e7_members as m
        inner join e7_key_stats as s using (source_name, e7_key)
        inner join e7_hub as h using (source_name, e7_key)
        where
            s.distinct_records > 1 and m.source_name || '|' || m.source_id <> h.hub_key
    ),

    all_edges as (
        select rk_a, rk_b, 'e1_hubspot_user' as edge_type, false as is_conflict
        from e1
        union all
        select rk_a, rk_b, 'e3_hubspot_br_candidacy', false
        from e3
        union all
        select rk_a, rk_b, 'e4_ts_officeholder', false
        from e4
        union all
        select rk_a, rk_b, 'e5_cluster', false
        from e5
        union all
        select rk_a, rk_b, 'e6_eo_bridge', false
        from e6
        union all
        select rk_a, rk_b, 'e7_within_source', is_conflict
        from e7
    )

select
    least(rk_a, rk_b) as record_key_1,
    greatest(rk_a, rk_b) as record_key_2,
    edge_type,
    bool_or(is_conflict) as is_conflict
from all_edges
where rk_a is not null and rk_b is not null and rk_a <> rk_b
group by 1, 2, 3
