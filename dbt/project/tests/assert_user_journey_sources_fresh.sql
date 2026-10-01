-- Fails when any source behind the user journey table had not loaded for two
-- days at its latest build. The build itself succeeds on stale inputs, so
-- without this a silently old table reads as a current one. Every source here
-- syncs at least daily, so two days is one missed sync plus slack.
--
-- Attached to the refresh log rather than the table so the log row is written
-- before this can fail, keeping the stale build on record.
with
    latest as (
        select *
        from {{ ref("user_journey_refresh_log") }}
        qualify row_number() over (order by refreshed_at desc) = 1
    ),

    loads as (
        select
            refreshed_at,
            stack(
                4,
                'gp_api',
                gp_api_loaded_at,
                'amplitude',
                amplitude_loaded_at,
                'hubspot',
                hubspot_loaded_at,
                'stripe',
                stripe_loaded_at
            ) as (source_name, loaded_at)
        from latest
    )

select source_name, loaded_at, refreshed_at
from loads
where loaded_at is null or loaded_at < refreshed_at - interval 2 days
