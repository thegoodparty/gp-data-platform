-- Person record universe: one row per record_key participating in person
-- identity. Must contain every key the edge model can emit — an edge endpoint
-- missing from labels_0 silently drops its neighbors during propagation — so
-- the cluster- and bridge-derived gp_api user ids are unioned in alongside
-- the staging user table.
with
    campaigns as (
        select cast(campaign_id as string) as campaign_id, user_id
        from {{ ref("campaigns") }}
        where is_latest_version and user_id is not null
    ),

    clustered as (
        select source_id, source_name, split(source_id, '__')[0] as gp_api_campaign_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
    ),

    record_keys as (
        select 'ballotready|' || cast(br_candidate_id as string) as record_key
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        union
        select 'ballotready|' || cast(br_candidate_id as string)
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
        union
        select 'gp_api|' || cast(id as string)
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        union
        -- E5 endpoint mirror: gp_api cluster members resolve to user ids via
        -- campaigns; a user missing from staging must still seed a label.
        select 'gp_api|' || cast(camp.user_id as string)
        from clustered as cc
        inner join campaigns as camp on camp.campaign_id = cc.gp_api_campaign_id
        where cc.source_name = 'gp_api'
        union
        -- E6 endpoint mirror, same rationale.
        select 'gp_api|' || cast(gp_api_user_id as string)
        from {{ ref("int__civics_elected_official_gp_api_bridge") }}
        where gp_api_user_id is not null
        union
        select 'hubspot|' || cast(id as string)
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        union
        select 'techspeed_officeholder|' || cast(ts_officeholder_id as string)
        from {{ ref("int__civics_elected_official_canonical_ids") }}
        where not ts_officeholder_id_is_reused
        union
        select source_name || '|' || source_id
        from clustered
        where source_name in ('techspeed', 'ddhq')
    )

select record_key, substring_index(record_key, '|', 1) as source_name
from record_keys
