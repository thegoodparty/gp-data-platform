-- Source records that should reach a gp-api user and do not. A Stripe customer
-- with no user paid us without an account, which is worth surfacing rather
-- than dropping.
--
-- HubSpot contributes nothing here by design: a contact with no user is a
-- prospect, and there are 362,299 contacts against about 72,200 users, so those
-- rows would bury the ones that matter. HubSpot non-coverage is counted from
-- the user side instead.
with
    matched_customers as (
        select distinct stripe_customer_id
        from {{ ref("int__user_resolved_keys") }}
        where stripe_customer_id is not null
    ),

    subscription_state as (
        select
            stripe_customer_id,
            true as has_subscription,
            max(
                case
                    when subscription_status in ('active', 'trialing', 'past_due')
                    then 1
                    else 0
                end
            )
            = 1 as has_active_subscription
        from {{ ref("stg_airbyte_source__stripe_api_subscriptions") }}
        where stripe_customer_id is not null
        group by stripe_customer_id
    ),

    -- What the unreachable customers actually paid. Without it the exception
    -- list is a row count, and a row count cannot say whether the gap matters.
    customer_revenue as (
        select
            stripe_customer_id,
            sum(amount_captured_usd) as lifetime_paid_usd,
            count(*) as payment_count
        from {{ ref("stg_airbyte_source__stripe_api_charges") }}
        where
            charge_status = 'succeeded'
            and is_captured
            and is_livemode
            and stripe_customer_id is not null
        group by stripe_customer_id
    ),

    -- An exact email edge would reach a user for about three quarters of these
    -- customers, carrying most of the stranded money. Published beside the
    -- exception rather than acted on: entity resolution owns closing it, the
    -- same way the HubSpot side is handled.
    user_emails as (
        select distinct lower(trim(email)) as user_email
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        where email is not null
    )

select
    'stripe' as source_name,
    c.id as source_id,
    'stripe_customer_no_user' as reason_code,
    -- the customers stream is still a pure passthrough that casts nothing, so
    -- the epoch conversion happens here
    timestamp_seconds(c.created) as first_seen_at,
    coalesce(s.has_subscription, false) as has_subscription,
    coalesce(s.has_active_subscription, false) as has_active_subscription,
    coalesce(r.lifetime_paid_usd, 0) as lifetime_paid_usd,
    coalesce(r.payment_count, 0) as payment_count,
    ue.user_email is not null as email_match_available
from {{ ref("stg_airbyte_source__stripe_api_customers") }} as c
left join subscription_state as s on s.stripe_customer_id = c.id
left join customer_revenue as r on r.stripe_customer_id = c.id
left join user_emails as ue on ue.user_email = lower(trim(c.email))
where
    c.id not in (
        select stripe_customer_id
        from matched_customers
        where stripe_customer_id is not null
    )
