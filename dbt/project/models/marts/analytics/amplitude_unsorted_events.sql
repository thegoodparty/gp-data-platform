{{ config(materialized="view") }}

-- Logged-in, non-machine events no rule or tag has sorted yet, per day and user,
-- so Sigma can count distinct users over a rolling window and list event types.
-- Anonymous marketing-site events are left out on purpose. Staff and test
-- accounts are dropped with the same person-keyed definition user_journey uses.
with
    internal_users as (
        select k.user_id
        from {{ ref("int__user_resolved_keys") }} as k
        inner join
            {{ ref("int__civics_internal_persons") }} as i
            on i.gp_person_id = k.gp_person_id
    )

select
    cast(e.event_time as date) as activity_date,
    e.event_type,
    try_cast(e.user_id as bigint) as user_id,
    count(*) as event_count
from {{ ref("int__amplitude_events_classified") }} as e
left anti join internal_users as i on i.user_id = try_cast(e.user_id as bigint)
where
    e.family = 'other'
    and not e.is_machine_emitted
    and try_cast(e.user_id as bigint) is not null
group by 1, 2, 3
