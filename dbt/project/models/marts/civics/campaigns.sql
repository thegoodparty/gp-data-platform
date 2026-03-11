with
    campaigns as (
        select * from {{ ref("stg_airbyte_internal__raw_gp_api_db_campaign") }}
    ),

    users as (select * from {{ ref("stg_airbyte_source__gp_api_db_user") }}),

    joined as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "c.id",
                        "c.details:electiondate::string",
                        "c.details:positionid::string",
                        "c.details:office::string",
                        "c.details:state::string",
                    ]
                )
            }} as campaign_version_id,
            c.id as campaign_id,
            c.slug as campaign_slug,

            c.data:hubspotid::string as hubspot_id,

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
            c.did_win,

            -- Pledge Status
            c.details:pledged::boolean as is_pledged,

            -- Election Context
            try_cast(c.details:electiondate::string as date) as election_date,
            c.details:state::string as campaign_state,
            c.details:office::string as campaign_office,
            c.details:party::string as campaign_party,
            c.details:level::string as election_level,

            -- BallotReady Position ID (decoded from base64)
            cast(
                regexp_extract(
                    cast(unbase64(c.details:positionid::string) as string),
                    '/([0-9]+)$',
                    1
                ) as bigint
            ) as ballotready_position_id,

            -- Version tracking
            row_number() over (
                partition by c.id
                order by c.updated_at desc nulls last, c._airbyte_extracted_at desc
            )
            = 1 as is_latest_version

        from campaigns c
        left join users u on c.user_id = u.id
    )

select *
from joined
