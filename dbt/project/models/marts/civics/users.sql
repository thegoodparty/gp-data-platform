with
    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    campaigns as (select * from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}),

    campaign_stats as (
        select
            user_id,
            count(*) as campaign_count,
            count(case when is_demo = false then 1 end) as non_demo_campaign_count,
            count(case when is_verified = true then 1 end) as verified_campaign_count,
            count(case when is_active = true then 1 end) as active_campaign_count,
            count(case when is_pro = true then 1 end) as pro_campaign_count,
            count(
                case when details:pledged::boolean = true then 1 end
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

            coalesce(cs.campaign_count, 0) as campaign_count,
            coalesce(cs.non_demo_campaign_count, 0) as non_demo_campaign_count,
            coalesce(cs.verified_campaign_count, 0) as verified_campaign_count,
            coalesce(cs.active_campaign_count, 0) as active_campaign_count,
            coalesce(cs.pro_campaign_count, 0) as pro_campaign_count,
            coalesce(cs.pledged_campaign_count, 0) as pledged_campaign_count,
            cs.first_campaign_created_at,
            cs.last_campaign_created_at,

            coalesce(cs.campaign_count > 0, false) as has_campaign,
            coalesce(cs.verified_campaign_count > 0, false) as has_verified_campaign,
            coalesce(cs.pledged_campaign_count > 0, false) as has_pledged_campaign
        from users u
        left join campaign_stats cs on u.id = cs.user_id
    )

select *
from final
