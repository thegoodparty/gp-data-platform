-- How far each user got through the onboarding and Pro upgrade flows, one row per
-- step reached in order. A step counts only when every earlier step fired first,
-- so a count of users per step is a funnel and never rises.
--
-- Core steps of each flow's current version, counted from the day it launched.
-- The flows have been rebuilt and edited since (optional questions added, screens
-- retired), and a step list spanning versions would read renames as drop-off.
-- Grain: one row per user_id, flow, step_order.
with
    steps as (
        select *
        from
        values
            (
                'win_onboarding',
                date '2026-06-08',
                1,
                'Welcome',
                'Onboarding V2 - Welcome Viewed'
            ),
            (
                'win_onboarding',
                date '2026-06-08',
                2,
                'Ballot status',
                'Onboarding V2 - Ballot Status Completed'
            ),
            (
                'win_onboarding',
                date '2026-06-08',
                3,
                'Party',
                'Onboarding V2 - Party Designation Completed'
            ),
            (
                'win_onboarding',
                date '2026-06-08',
                4,
                'Office',
                'Onboarding V2 - Office Completed'
            ),
            (
                'win_onboarding',
                date '2026-06-08',
                5,
                'Votes needed',
                'Onboarding V2 - Votes Needed Completed'
            ),
            (
                'win_onboarding',
                date '2026-06-08',
                6,
                'Pledge',
                'Onboarding V2 - Pledge Completed'
            ),
            -- "Not yet filed" at the first screen ends the flow, so most of the
            -- drop to Guidance is people who haven't filed, not people who quit.
            (
                'pro_upgrade',
                date '2026-06-17',
                1,
                'Filing status',
                'Pro Upgrade - Filing Status Viewed'
            ),
            (
                'pro_upgrade',
                date '2026-06-17',
                2,
                'Guidance',
                'Pro Upgrade - Guidance Viewed'
            ),
            ('pro_upgrade', date '2026-06-17', 3, 'EIN', 'Pro Upgrade - EIN Viewed'),
            (
                'pro_upgrade',
                date '2026-06-17',
                4,
                'Filing details',
                'Pro Upgrade - Filing Details Viewed'
            ),
            (
                'pro_upgrade',
                date '2026-06-17',
                5,
                'Payment',
                'Pro Upgrade - Payment Viewed'
            ),
            -- Server-side from the Stripe webhook, so it survives ad blockers.
            ('pro_upgrade', date '2026-06-17', 6, 'Paid', 'pro_upgrade_complete'),
            (
                'serve_onboarding',
                date '2026-06-24',
                1,
                'Welcome',
                'Serve Onboarding - Welcome Viewed'
            ),
            (
                'serve_onboarding',
                date '2026-06-24',
                2,
                'Office status',
                'Serve Onboarding - Office Status Viewed'
            ),
            (
                'serve_onboarding',
                date '2026-06-24',
                3,
                'Party',
                'Serve Onboarding - Party Designation Viewed'
            ),
            (
                'serve_onboarding',
                date '2026-06-24',
                4,
                'Term dates',
                'Serve Onboarding - Term Dates Viewed'
            ),
            (
                'serve_onboarding',
                date '2026-06-24',
                5,
                'Know your constituents',
                'Serve Onboarding - Know Your Constituents Viewed'
            ),
            (
                'serve_onboarding',
                date '2026-06-24',
                6,
                'Pledge',
                'Serve Onboarding - Pledge Completed'
            ) as t(flow, flow_launched_on, step_order, step, event_type)
    ),

    first_fired as (
        select
            try_cast(e.user_id as bigint) as user_id,
            s.flow,
            s.step_order,
            s.step,
            min(e.event_time) as step_at
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        join steps as s on e.event_type = s.event_type
        where
            e.event_time >= s.flow_launched_on
            and try_cast(e.user_id as bigint) is not null
        group by all
    ),

    ordered as (
        select
            *,
            -- In order: no earlier step missing, and none fired after this one.
            row_number() over (partition by user_id, flow order by step_order)
            = step_order
            and coalesce(
                step_at >= max(step_at) over (
                    partition by user_id, flow
                    order by step_order
                    rows between unbounded preceding and 1 preceding
                ),
                true
            ) as is_in_order,
            lag(step_at) over (
                partition by user_id, flow order by step_order
            ) as previous_step_at
        from first_fired
    ),

    reached as (
        select
            *,
            -- One out-of-order step ends the user's funnel there.
            bool_and(is_in_order) over (
                partition by user_id, flow
                order by step_order
                rows between unbounded preceding and current row
            ) as is_reached
        from ordered
    )

select
    user_id,
    flow,
    step_order,
    step,
    step_at,
    unix_timestamp(step_at)
    - unix_timestamp(previous_step_at) as seconds_from_previous_step
from reached
where is_reached
