-- Completed Stripe Checkout Sessions, one row per session. Since 2026-02-16 the
-- product sells one-offs through Checkout Sessions in payment mode, and the
-- purchase metadata lives on the session, not on the charge, so this is how a
-- post-February charge gets its purchase type.
select
    checkout_session_id,
    payment_intent_id,
    checkout_mode,
    payment_type,
    purchase_type,
    amount_total_usd,
    created_at
from {{ ref("stg_airbyte_source_finance__stripe_api_checkout_sessions") }}
where checkout_status = 'complete' and is_livemode
