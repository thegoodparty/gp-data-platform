-- Fails for any user whose dated Payment rows do not add up to
-- user_journey.lifetime_paid_usd. Both read the same typed charges, so a gap
-- means one side changed its filter. Refunds are not compared: the refunds
-- stream and a charge's refunded amount can differ while a refund is pending.
with
    events as (
        select user_id, sum(event_amount_usd) as paid
        from {{ ref("user_journey_events") }}
        where event = 'Payment'
        group by user_id
    ),

    journey as (
        select user_id, lifetime_paid_usd
        from {{ ref("user_journey") }}
        where lifetime_paid_usd > 0
    )

select
    coalesce(j.user_id, e.user_id) as user_id,
    j.lifetime_paid_usd,
    coalesce(e.paid, 0) as events_paid
from journey as j
full join events as e using (user_id)
where abs(coalesce(j.lifetime_paid_usd, 0) - coalesce(e.paid, 0)) > 0.01
