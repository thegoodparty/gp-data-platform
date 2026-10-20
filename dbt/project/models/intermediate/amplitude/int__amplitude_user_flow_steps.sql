-- How far each user got through the onboarding and Pro upgrade flows, one row per
-- step reached in order. A step counts only when every earlier step fired first,
-- so a count of users per step is a funnel and never rises.
--
-- Core steps of each flow's current version, counted from the day it launched.
-- The flows have been rebuilt and edited since (optional questions added, screens
-- retired), and a step list spanning versions would read renames as drop-off.
-- win_onboarding_core is the exception: the three steps every Win onboarding
-- version has had, so a funnel can reach back before the current version.
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

    -- Each event counts only while its version ran (to the day before retired_on):
    -- the old office events still fire outside onboarding, about 25 users a month
    -- since the current version launched. The old and short-lived versions ran side
    -- by side from 2026-05-07, and both stopped firing by 2026-06-10.
    core_steps as (
        select *
        from
        values
            (
                'win_onboarding_core',
                date '2025-05-28',
                date '2026-06-11',
                1,
                'Office',
                'Onboarding - Office Step: Click Next'
            ),
            (
                'win_onboarding_core',
                date '2025-06-26',
                date '2026-06-11',
                1,
                'Office',
                'Onboarding - Candidate Office Completed'
            ),
            (
                'win_onboarding_core',
                date '2026-05-07',
                date '2026-06-11',
                1,
                'Office',
                'Onboarding - Office Selection Completed'
            ),
            (
                'win_onboarding_core',
                date '2026-06-08',
                cast(null as date),
                1,
                'Office',
                'Onboarding V2 - Office Completed'
            ),
            (
                'win_onboarding_core',
                date '2025-05-28',
                date '2026-06-11',
                2,
                'Party',
                'Onboarding - Party Step: Click Submit'
            ),
            (
                'win_onboarding_core',
                date '2025-09-17',
                date '2026-06-11',
                2,
                'Party',
                'Onboarding - Candidate Affiliation Completed'
            ),
            (
                'win_onboarding_core',
                date '2026-05-07',
                date '2026-06-11',
                2,
                'Party',
                'Onboarding - Party Selection Completed'
            ),
            (
                'win_onboarding_core',
                date '2026-06-08',
                cast(null as date),
                2,
                'Party',
                'Onboarding V2 - Party Designation Completed'
            ),
            (
                'win_onboarding_core',
                date '2025-05-28',
                date '2026-06-11',
                3,
                'Pledge',
                'Onboarding - Pledge Step: Click Submit'
            ),
            (
                'win_onboarding_core',
                date '2025-09-17',
                date '2026-06-11',
                3,
                'Pledge',
                'Onboarding - Candidate Pledge Completed'
            ),
            (
                'win_onboarding_core',
                date '2026-05-07',
                date '2026-06-11',
                3,
                'Pledge',
                'Onboarding - Pledge Completed'
            ),
            (
                'win_onboarding_core',
                date '2026-06-08',
                cast(null as date),
                3,
                'Pledge',
                'Onboarding V2 - Pledge Completed'
            ) as t(flow, flow_launched_on, retired_on, step_order, step, event_type)
    ),

    all_steps as (
        select
            flow,
            flow_launched_on,
            cast(null as date) as retired_on,
            step_order,
            step,
            event_type
        from steps
        union all
        select flow, flow_launched_on, retired_on, step_order, step, event_type
        from core_steps
    ),

    first_fired as (
        select
            try_cast(e.user_id as bigint) as user_id,
            s.flow,
            s.step_order,
            s.step,
            min(e.event_time) as step_at
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        join all_steps as s on e.event_type = s.event_type
        where
            e.event_time >= s.flow_launched_on
            and (s.retired_on is null or e.event_time < s.retired_on)
            and try_cast(e.user_id as bigint) is not null
        group by all
    ),

    ordered as (
        select
            *,
            max(step_at) over (
                partition by user_id, flow
                order by step_order
                rows between unbounded preceding and 1 preceding
            ) as previous_step_at,
            row_number() over (
                partition by user_id, flow order by step_order
            ) as step_rank
        from first_fired
    ),

    checked as (
        select
            *,
            -- In order: no earlier step missing, and none fired after this one. The
            -- core onboarding flow checks presence only, because party moved ahead
            -- of office in 2026.
            step_rank = step_order
            and (
                flow = 'win_onboarding_core'
                or coalesce(step_at >= previous_step_at, true)
            ) as is_in_order
        from ordered
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
        from checked
    )

select
    user_id,
    flow,
    step_order,
    step,
    step_at,
    -- Null when a core onboarding step came before the one listed above it.
    case
        when step_at >= previous_step_at
        then unix_timestamp(step_at) - unix_timestamp(previous_step_at)
    end as seconds_from_previous_step
from reached
where is_reached
