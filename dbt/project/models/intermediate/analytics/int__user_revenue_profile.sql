-- The revenue slice of the end-to-end user view: what a user has paid us and
-- what state their subscription is in, at gp-api user grain. One row per user
-- whether or not they ever paid.
--
-- Money is read from charges, not invoices. Invoices cover the Pro
-- subscription and nothing else, so an invoice-only total reports a user who
-- bought a domain, a text-outreach package or a sales-led plan as having paid
-- nothing. Charges see all of it: subscription revenue is about a third of
-- what this account has collected from users who reach a row here.
--
-- Table, not view: a six-relation join with two aggregations, read by the wide
-- journey table and by its tests.
{{ config(materialized="table") }}

with
    keys as (
        select user_id, stripe_customer_id, stripe_customer_count
        from {{ ref("int__user_resolved_keys") }}
    ),

    pro_state as (select user_id, pro_campaign_count from {{ ref("users") }}),

    -- Succeeded and captured only. A failed or uncaptured charge is an
    -- attempt, and the two together are a quarter of all charge rows.
    settled_charges as (
        select
            charge_id,
            stripe_customer_id,
            product_user_id,
            invoice_id,
            payment_type,
            hs_checkout_session_id,
            description,
            created_at,
            amount_captured_usd,
            amount_refunded_usd
        from {{ ref("stg_airbyte_source__stripe_api_charges") }}
        where charge_status = 'succeeded' and is_captured
    ),

    -- Two ways a charge reaches a user, unioned on the charge id so a charge
    -- carrying both is counted once. The metadata leg recovers 98 charges
    -- taken with no Stripe customer at all, which the customer join cannot
    -- see.
    user_charges as (
        select k.user_id, c.charge_id
        from keys as k
        join settled_charges as c on c.stripe_customer_id = k.stripe_customer_id
        union
        select p.user_id, c.charge_id
        from settled_charges as c
        join pro_state as p on p.user_id = c.product_user_id
        where c.stripe_customer_id is null
    ),

    -- Purchase lines. Only the first three are nameable: on 2026-02-16 the
    -- product's checkout moved behind a payment intent this connector does not
    -- ingest, and its metadata went with it, so every own-product one-off
    -- since then arrives with nothing on it but an amount. The dollars are
    -- still the user's; only the line is unknown.
    classified as (
        select
            uc.user_id,
            c.charge_id,
            c.created_at,
            c.amount_captured_usd,
            c.amount_refunded_usd,
            case
                when c.invoice_id is not null or c.description like 'Subscription%'
                then 'subscription'
                when c.hs_checkout_session_id is not null
                then 'sales_led'
                when c.payment_type is not null
                then 'one_off'
                else 'unclassified'
            end as revenue_line
        from user_charges as uc
        join settled_charges as c using (charge_id)
    ),

    revenue as (
        select
            user_id,
            count(*) as payment_count,
            sum(amount_captured_usd) as lifetime_paid_usd,
            sum(amount_refunded_usd) as lifetime_refunded_usd,
            sum(
                case
                    when revenue_line = 'subscription' then amount_captured_usd else 0
                end
            ) as subscription_paid_usd,
            sum(
                case when revenue_line = 'one_off' then amount_captured_usd else 0 end
            ) as one_off_paid_usd,
            sum(
                case when revenue_line = 'sales_led' then amount_captured_usd else 0 end
            ) as sales_led_paid_usd,
            sum(
                case
                    when revenue_line = 'unclassified' then amount_captured_usd else 0
                end
            ) as unclassified_paid_usd,
            min(created_at) as first_payment_at,
            max(created_at) as last_payment_at
        from classified
        group by user_id
    ),

    -- One customer can hold several subscriptions. Current state is the live
    -- one where there is one, and otherwise the most recent, so a churned user
    -- keeps the subscription they churned out of rather than going null.
    ranked_subscriptions as (
        select
            stripe_customer_id,
            subscription_status,
            started_at,
            canceled_at,
            ended_at,
            will_cancel_at_period_end,
            current_period_end_at,
            plan_amount_usd,
            count(*) over (partition by stripe_customer_id) as subscription_count,
            row_number() over (
                partition by stripe_customer_id
                order by
                    case
                        when subscription_status in ('active', 'trialing', 'past_due')
                        then 0
                        else 1
                    end,
                    created_at desc
            ) as rn
        from {{ ref("stg_airbyte_source__stripe_api_subscriptions") }}
        where stripe_customer_id is not null
    ),

    current_subscription as (select * from ranked_subscriptions where rn = 1),

    -- The campaign archive begins 2026-04-13, so a user already Pro at first
    -- sighting has no observable transition and is excluded: taking their
    -- first snapshot as a start date would date 1,754 users to the day the
    -- archive opened. Only the 95 that flipped while under observation count.
    pro_versions as (
        select
            user_id,
            min(_airbyte_extracted_at) as first_seen_at,
            min(case when is_pro then _airbyte_extracted_at end) as first_pro_seen_at
        from {{ ref("int__civics_campaign_versions") }}
        group by user_id
    ),

    pro_transition as (
        select user_id, first_pro_seen_at
        from pro_versions
        where first_pro_seen_at > first_seen_at
    )

select
    k.user_id,
    coalesce(p.pro_campaign_count, 0) > 0 as is_pro,
    coalesce(p.pro_campaign_count, 0) as pro_campaign_count,

    -- Stripe dates half the Pro population and the archive adds a sliver; the
    -- rest were flagged Pro by a route that left no timestamp anywhere. The
    -- source column is published beside the date because the two routes do not
    -- mean the same thing: one is when they paid, the other is when we first
    -- saw the flag on.
    case
        when coalesce(p.pro_campaign_count, 0) = 0
        then null
        when r.first_payment_at is not null
        then r.first_payment_at
        else pt.first_pro_seen_at
    end as pro_since,
    case
        when coalesce(p.pro_campaign_count, 0) = 0
        then null
        when r.first_payment_at is not null
        then 'stripe_first_payment'
        when pt.first_pro_seen_at is not null
        then 'campaign_version_change'
        else 'undated'
    end as pro_since_source,

    k.stripe_customer_id is not null as has_stripe_customer,
    -- How many user rows share this customer id, 41 of which sit on two. A
    -- person-grain rollup of the money columns has to dedupe on it.
    k.stripe_customer_count as shared_stripe_customer_user_count,

    -- Zero where we can see Stripe and nothing was paid, zero as well where
    -- there is no customer at all: a user with no Stripe customer has paid us
    -- nothing, which is a fact and not an absence.
    coalesce(r.payment_count, 0) as payment_count,
    coalesce(r.lifetime_paid_usd, 0) as lifetime_paid_usd,
    coalesce(r.lifetime_refunded_usd, 0) as lifetime_refunded_usd,
    coalesce(r.lifetime_paid_usd, 0)
    - coalesce(r.lifetime_refunded_usd, 0) as lifetime_net_usd,
    coalesce(r.subscription_paid_usd, 0) as subscription_paid_usd,
    coalesce(r.one_off_paid_usd, 0) as one_off_paid_usd,
    coalesce(r.sales_led_paid_usd, 0) as sales_led_paid_usd,
    coalesce(r.unclassified_paid_usd, 0) as unclassified_paid_usd,
    r.first_payment_at,
    r.last_payment_at,

    s.subscription_status,
    coalesce(s.subscription_count, 0) as subscription_count,
    coalesce(
        s.subscription_status in ('active', 'trialing', 'past_due'), false
    ) as has_active_subscription,
    s.started_at as subscription_started_at,
    s.canceled_at as subscription_canceled_at,
    s.ended_at as subscription_ended_at,
    s.current_period_end_at as subscription_period_end_at,
    s.will_cancel_at_period_end as subscription_will_cancel_at_period_end,
    s.plan_amount_usd as subscription_plan_amount_usd
from keys as k
left join pro_state as p using (user_id)
left join revenue as r using (user_id)
left join pro_transition as pt using (user_id)
left join current_subscription as s on s.stripe_customer_id = k.stripe_customer_id
