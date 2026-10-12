-- Fails where a Serve growth state appears before the user's first Serve month,
-- or is missing from it on. Before then the user is not a Serve user at all, and
-- reading them as dormant would put every Win user in the Serve chart.
with
    m as (
        select
            *,
            min(case when serve_activity_days > 0 then activity_month end) over (
                partition by user_id
            ) as first_serve_month
        from {{ ref("int__user_activity_monthly") }}
    )

select user_id, activity_month, first_serve_month, serve_growth_state
from m
where
    (
        (first_serve_month is null or activity_month < first_serve_month)
        and serve_growth_state is not null
    )
    or (activity_month >= first_serve_month and serve_growth_state is null)
