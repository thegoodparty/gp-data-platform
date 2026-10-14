-- Each Win user's usage group, one label per user so a dashboard control can
-- filter every page to the same people. The groups run from deepest to
-- shallowest use and are exclusive: power user, sent outreach without reaching
-- the power-user bar, paid for Pro without sending, and neither.
--
-- Moves with the build date, because is_power_user does.
with
    win_users as (select user_id from {{ ref("users") }} where is_win_user),

    power as (select user_id, is_power_user from {{ ref("int__user_power_user") }}),

    -- Read sends directly rather than power_user_has_sent, which only covers users
    -- with a candidacy and stops at the election.
    senders as (
        select user_id
        from {{ ref("int__user_engagement_days") }}
        group by user_id
        having bool_or(has_activation_event)
    ),

    -- First subscription payment, not first payment: a domain purchase alone is
    -- not Pro.
    revenue as (
        select user_id, first_subscription_payment_at
        from {{ ref("int__user_revenue_profile") }}
    )

select
    w.user_id,
    case
        when p.is_power_user
        then 'Power user'
        when snd.user_id is not null
        then 'Sent, not a power user'
        when r.first_subscription_payment_at is not null
        then 'Pro, never sent'
        else 'Free, never sent'
    end as usage_group
from win_users as w
left join power as p using (user_id)
left join senders as snd using (user_id)
left join revenue as r using (user_id)
