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
            customer,
            true as has_subscription,
            max(
                case when status in ('active', 'trialing', 'past_due') then 1 else 0 end
            )
            = 1 as has_active_subscription
        from {{ ref("stg_airbyte_source__stripe_api_subscriptions") }}
        where customer is not null
        group by customer
    )

select
    'stripe' as source_name,
    c.id as source_id,
    'stripe_customer_no_user' as reason_code,
    -- the Stripe connector lands `created` as epoch seconds and the staging
    -- layer is a pure passthrough that casts nothing, so the conversion
    -- happens here
    timestamp_seconds(c.created) as first_seen_at,
    coalesce(s.has_subscription, false) as has_subscription,
    coalesce(s.has_active_subscription, false) as has_active_subscription
from {{ ref("stg_airbyte_source__stripe_api_customers") }} as c
left join subscription_state as s on s.customer = c.id
where
    c.id not in (
        select stripe_customer_id
        from matched_customers
        where stripe_customer_id is not null
    )
