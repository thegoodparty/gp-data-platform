-- Dated money moments per user: each payment, each refund, and each Pro
-- subscription's start and end. Feeds user_journey_events, so revenue can be
-- counted by period rather than only as lifetime totals.
--
-- Amounts are signed so a sum is net revenue: payments positive, refunds
-- negative. A Stripe customer shared by two user rows puts its rows on both;
-- sum across users with user_journey.is_primary_stripe_customer_row.
with
    charges as (
        select user_id, charge_id, created_at, amount_captured_usd, revenue_line
        from {{ ref("int__user_stripe_charges") }}
    ),

    -- Converted here because the staging model stays raw: the finance mart
    -- passes its columns straight through.
    refunds as (
        select
            cast(id as string) as refund_id,
            cast(charge as string) as charge_id,
            timestamp_seconds(cast(created as bigint)) as refunded_at,
            cast(amount / 100.0 as decimal(12, 2)) as amount_usd
        from {{ ref("stg_airbyte_source__stripe_api_refunds") }}
        where status = 'succeeded'
    ),

    keys as (
        select user_id, stripe_customer_id
        from {{ ref("int__user_resolved_keys") }}
        where stripe_customer_id is not null
    ),

    -- incomplete and incomplete_expired are failed checkouts that never
    -- started, so they are neither a start nor a churn.
    subscriptions as (
        select
            subscription_id,
            stripe_customer_id,
            started_at,
            ended_at,
            plan_interval,
            plan_amount_usd
        from {{ ref("stg_airbyte_source__stripe_api_subscriptions") }}
        where
            is_livemode
            and subscription_status not in ('incomplete', 'incomplete_expired')
    ),

    user_subscriptions as (
        select k.user_id, s.*
        from subscriptions as s
        join keys as k using (stripe_customer_id)
    )

select
    user_id,
    'Payment' as event,
    'charge:' || charge_id as event_key,
    created_at as event_at,
    revenue_line as event_label,
    amount_captured_usd as event_amount_usd
from charges
union all
-- Dated when refunded, not when charged, so a period's net revenue is what
-- moved in that period.
select
    c.user_id,
    'Refund',
    'refund:' || r.refund_id,
    r.refunded_at,
    c.revenue_line,
    - r.amount_usd
from refunds as r
join charges as c using (charge_id)
union all
select
    user_id,
    'Subscription started',
    'subscription_start:' || subscription_id,
    started_at,
    plan_interval,
    plan_amount_usd
from user_subscriptions
union all
-- Ended, not cancelled: a subscription set to cancel at period end keeps
-- paying until then.
select
    user_id,
    'Subscription ended',
    'subscription_end:' || subscription_id,
    ended_at,
    plan_interval,
    plan_amount_usd
from user_subscriptions
where ended_at is not null
