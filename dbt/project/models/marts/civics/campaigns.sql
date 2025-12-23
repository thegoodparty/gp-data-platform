with
    campaigns as (select * from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}),

    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    joined as (
        select
            c.id as campaign_id,
            c.slug as campaign_slug,

            c.data:hubspotid::string as hubspot_id,

            u.id as fake_id,
            u.id as user_id,
            u.email as user_email,
            u.first_name as user_first_name,
            u.last_name as user_last_name,
            u.phone as user_phone,
            u.zip as user_zip,

            -- User Signup Metrics
            u.created_at as user_created_at,

            -- Campaign Creation Metrics
            c.created_at as created_at,
            c.updated_at as updated_at,

            -- Verification Status
            coalesce(c.is_verified, false) as is_verified,

            -- Campaign Status Flags
            c.is_active,
            c.is_pro,
            c.is_demo,

            -- Pledge Status
            c.details:pledged::boolean as is_pledged,

            -- Election Context
            try_cast(c.details:electiondate::string as date) as election_date,
            c.details:state::string as campaign_state,
            c.details:office::string as campaign_office,
            c.details:party::string as campaign_party,
            c.details:level::string as election_level

        from campaigns c
        left join users u on c.user_id = u.id
    )

select *
from joined
