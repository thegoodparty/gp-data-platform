{{ config(materialized="view") }}

-- Logged-in, non-machine events no rule or tag has sorted yet, by month.
-- Anonymous marketing-site events are left out on purpose.
select
    trunc(event_time, 'MM') as activity_month,
    event_type,
    count(distinct try_cast(user_id as bigint)) as user_count,
    count(*) as event_count
from {{ ref("int__amplitude_events_classified") }}
where
    family = 'other'
    and not is_machine_emitted
    and try_cast(user_id as bigint) is not null
group by 1, 2
