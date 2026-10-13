-- Each settled Stripe charge that reaches a gp-api user, with its purchase line.
-- One row per user and charge: a Stripe customer shared by two user rows puts
-- its charges on both, so a sum across users filters on
-- int__user_revenue_profile.is_primary_stripe_customer_row.
--
-- The one place a charge gets its revenue line. int__user_revenue_profile
-- totals these rows and int__user_revenue_events dates them, so the two always
-- agree on what counts as subscription money.
with
    keys as (
        select user_id, stripe_customer_id from {{ ref("int__user_resolved_keys") }}
    ),

    users as (select user_id from {{ ref("users") }}),

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
        where charge_status = 'succeeded' and is_captured and is_livemode
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
        select u.user_id, c.charge_id
        from settled_charges as c
        join users as u on u.user_id = c.product_user_id
        where c.stripe_customer_id is null
    )

-- Purchase lines. Only the first three are nameable: on 2026-02-16 the
-- product's checkout moved behind a payment intent this connector does not
-- ingest, and its metadata went with it, so every own-product one-off since
-- then arrives with nothing on it but an amount. The dollars are still the
-- user's; only the line is unknown.
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
