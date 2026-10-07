-- Person links. One row per undirected pair of person records (unique_id in
-- int__er_prematch_people) that a native identifier asserts are the same
-- person: E1 HubSpot<->gp_api, E3 HubSpot->BR candidacy, E4
-- ts_officeholder->BR, E6 the gp_api->BR elected-official bridge, and E5
-- candidacy-stage cluster co-membership (hub to the cluster's min record key).
-- matcha clusters these with the scored pairs as certain matches; no closure
-- happens in dbt. Both endpoints must be prematch records, so the universe
-- matcha clusters over and the one the mint reads are the same set.
with
    prematch as (select unique_id from {{ ref("int__er_prematch_people") }}),

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

    -- E5: hub every member to the cluster's min record key. Hub-and-spoke is
    -- enough because clustering reaches the rest. Clusters spanning two BR
    -- people are already absent from the members model.
    cluster_hub as (
        select cluster_id, min(record_key) as hub_key
        from {{ ref("int__civics_candidacy_cluster_members") }}
        group by cluster_id
    ),

    e5 as (
        select m.record_key as rk_a, h.hub_key as rk_b
        from {{ ref("int__civics_candidacy_cluster_members") }} as m
        inner join cluster_hub as h using (cluster_id)
        where m.record_key <> h.hub_key
    ),

    -- E6: elected-official bridge. gp_api user <-> BR person.
    e6 as (
        select distinct
            'gp_api|' || cast(gp_api_user_id as string) as rk_a,
            'ballotready|' || cast(br_candidate_id as string) as rk_b
        from {{ ref("int__civics_elected_official_gp_api_bridge") }}
        where gp_api_user_id is not null and br_candidate_id is not null
    ),

    all_pairs as (
        select rk_a, rk_b, 'e1_hubspot_user' as link_type
        from e1
        union all
        select rk_a, rk_b, 'e3_hubspot_br_candidacy'
        from e3
        union all
        select rk_a, rk_b, 'e4_ts_officeholder'
        from e4
        union all
        select rk_a, rk_b, 'e5_candidacy_cluster'
        from e5
        union all
        select rk_a, rk_b, 'e6_eo_bridge'
        from e6
    )

select distinct
    least(p.rk_a, p.rk_b) as unique_id_l,
    greatest(p.rk_a, p.rk_b) as unique_id_r,
    p.link_type
from all_pairs as p
inner join prematch as a on a.unique_id = p.rk_a
inner join prematch as b on b.unique_id = p.rk_b
where p.rk_a <> p.rk_b
