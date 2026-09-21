with

    source as (select * from {{ source("airbyte_source", "anthropic_api_summaries") }}),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(starting_at as timestamp) as starting_at,
            cast(ending_at as timestamp) as ending_at,
            cast(assigned_seat_count as bigint) as assigned_seat_count,
            cast(pending_invite_count as bigint) as pending_invite_count,
            cast(daily_active_user_count as bigint) as daily_active_user_count,
            cast(weekly_active_user_count as bigint) as weekly_active_user_count,
            cast(monthly_active_user_count as bigint) as monthly_active_user_count,
            -- percentages of assigned seats, 0 to 100
            cast(daily_adoption_rate as double) as daily_adoption_rate,
            cast(weekly_adoption_rate as double) as weekly_adoption_rate,
            cast(monthly_adoption_rate as double) as monthly_adoption_rate,
            cast(
                chat_daily_active_user_count as bigint
            ) as chat_daily_active_user_count,
            cast(
                chat_weekly_active_user_count as bigint
            ) as chat_weekly_active_user_count,
            cast(
                chat_monthly_active_user_count as bigint
            ) as chat_monthly_active_user_count,
            cast(
                claude_code_daily_active_user_count as bigint
            ) as claude_code_daily_active_user_count,
            cast(
                claude_code_weekly_active_user_count as bigint
            ) as claude_code_weekly_active_user_count,
            cast(
                claude_code_monthly_active_user_count as bigint
            ) as claude_code_monthly_active_user_count,
            cast(
                cowork_daily_active_user_count as bigint
            ) as cowork_daily_active_user_count,
            cast(
                cowork_weekly_active_user_count as bigint
            ) as cowork_weekly_active_user_count,
            cast(
                cowork_monthly_active_user_count as bigint
            ) as cowork_monthly_active_user_count,
            cast(
                claude_design_daily_active_user_count as bigint
            ) as claude_design_daily_active_user_count,
            cast(
                claude_design_weekly_active_user_count as bigint
            ) as claude_design_weekly_active_user_count,
            cast(
                claude_design_monthly_active_user_count as bigint
            ) as claude_design_monthly_active_user_count,
            cast(
                office_agent_daily_active_user_count as bigint
            ) as office_agent_daily_active_user_count,
            cast(
                office_agent_weekly_active_user_count as bigint
            ) as office_agent_weekly_active_user_count,
            cast(
                office_agent_monthly_active_user_count as bigint
            ) as office_agent_monthly_active_user_count,
            cast(
                science_daily_active_user_count as bigint
            ) as science_daily_active_user_count,
            cast(
                science_weekly_active_user_count as bigint
            ) as science_weekly_active_user_count,
            cast(
                science_monthly_active_user_count as bigint
            ) as science_monthly_active_user_count,
            cast(science_entitled_user_count as bigint) as science_entitled_user_count

        from source

    )

select *
from renamed
