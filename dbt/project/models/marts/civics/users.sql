{{ config(materialized="table") }}

with
    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    campaigns as (select * from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}),

    organizations as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_organization") }}
    ),

    elected_offices as (
        select * from {{ ref("stg_airbyte_source__gp_api_db_elected_office") }}
    ),

    polls as (select * from {{ ref("stg_airbyte_source__gp_api_db_poll") }}),

    poll_users as (
        /*
            Users who have completed at least one poll through an elected
            office. eo_activated_at is the earliest poll creation date.
        */
        select eo.user_id, min(p.created_at) as eo_activated_at
        from elected_offices eo
        inner join polls p on eo.id = p.elected_office_id
        where p.is_completed
        group by eo.user_id
    ),

    organization_stats as (
        select
            o.owner_id as user_id,
            count(*) as organization_count,
            count(c.id) as win_organization_count,
            count(eo.id) as serve_organization_count,
            min(
                case when eo.id is not null then o.created_at end
            ) as first_serve_org_created_at
        from organizations o
        left join campaigns c on o.slug = c.organization_slug
        left join elected_offices eo on o.slug = eo.organization_slug
        group by o.owner_id
    ),

    campaign_stats as (
        select
            user_id,
            count(*) as campaign_count,
            count(case when not is_demo then 1 end) as non_demo_campaign_count,
            count(case when is_verified then 1 end) as verified_campaign_count,
            count(case when is_active then 1 end) as active_campaign_count,
            count(case when is_pro then 1 end) as pro_campaign_count,
            count(
                case when details:pledged::boolean then 1 end
            ) as pledged_campaign_count,
            min(created_at) as first_campaign_created_at,
            max(created_at) as last_campaign_created_at
        from campaigns
        group by user_id
    ),

    final as (
        select
            u.id as user_id,

            u.email,
            u.first_name,
            u.last_name,
            u.phone,
            u.zip,

            u.created_at,
            u.updated_at,

            coalesce(os.organization_count, 0) as organization_count,
            coalesce(os.win_organization_count, 0) as win_organization_count,
            coalesce(os.serve_organization_count, 0) as serve_organization_count,

            coalesce(cs.campaign_count, 0) as campaign_count,
            coalesce(cs.non_demo_campaign_count, 0) as non_demo_campaign_count,
            coalesce(cs.verified_campaign_count, 0) as verified_campaign_count,
            coalesce(cs.active_campaign_count, 0) as active_campaign_count,
            coalesce(cs.pro_campaign_count, 0) as pro_campaign_count,
            coalesce(cs.pledged_campaign_count, 0) as pledged_campaign_count,
            cs.first_campaign_created_at,
            cs.last_campaign_created_at,

            coalesce(cs.verified_campaign_count > 0, false) as has_verified_campaign,
            coalesce(cs.pledged_campaign_count > 0, false) as has_pledged_campaign,

            coalesce(os.win_organization_count > 0, false) as is_win_user,

            coalesce(
                os.serve_organization_count > 0 or pu.user_id is not null, false
            ) as is_serve_user,
            case when pu.user_id is not null then true else false end as is_poll_user,
            least(pu.eo_activated_at, os.first_serve_org_created_at) as eo_activated_at
        from users u
        left join organization_stats os on u.id = os.user_id
        left join campaign_stats cs on u.id = cs.user_id
        left join poll_users pu on u.id = pu.user_id
    )

select *
from final
