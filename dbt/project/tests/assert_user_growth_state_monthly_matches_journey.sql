-- Fails for any user whose current month in the cohort history disagrees with
-- the growth_state on their user_journey row. Both read the same monthly model,
-- so a failure means one of them stopped doing so.
with
    current_month as (
        select user_id, growth_state
        from {{ ref("user_growth_state_monthly") }}
        where is_partial_month
    ),

    journey as (
        select user_id, growth_state
        from {{ ref("user_journey") }}
        where growth_state is not null
    )

select
    coalesce(j.user_id, m.user_id) as user_id,
    j.growth_state as journey_growth_state,
    m.growth_state as monthly_growth_state
from journey as j
full outer join current_month as m on j.user_id = m.user_id
where j.growth_state is distinct from m.growth_state
