-- Completed Stripe Checkout Sessions, one row per session, read from the events
-- stream. Since 2026-02-16 the product sells one-offs through Checkout Sessions
-- in payment mode, and the purchase metadata lives on the session, not on the
-- charge, so this is how a post-February charge gets its purchase type.
--
-- Parsed here because the events staging model stays raw: the finance mart
-- passes its columns straight through. Stripe keeps events for 30 days, so this
-- history exists only as long as the connector is never reset.
with
    events as (
        select data, created
        from {{ ref("stg_airbyte_source__stripe_api_events") }}
        where
            livemode
            and type in (
                'checkout.session.completed', 'checkout.session.async_payment_succeeded'
            )
    )

select
    get_json_object(data, '$.object.id') as checkout_session_id,
    get_json_object(data, '$.object.payment_intent') as payment_intent_id,
    get_json_object(data, '$.object.mode') as checkout_mode,
    get_json_object(data, '$.object.metadata.paymentType') as payment_type,
    get_json_object(data, '$.object.metadata.purchaseType') as purchase_type,
    cast(
        try_cast(get_json_object(data, '$.object.amount_total') as bigint)
        / 100.0 as decimal(12, 2)
    ) as amount_total_usd,
    timestamp_seconds(cast(created as bigint)) as completed_at
from events
-- An async payment fires a second event for the same session.
qualify
    row_number() over (
        partition by get_json_object(data, '$.object.id') order by created desc
    )
    = 1
