-- Stripe Checkout Sessions, one row per session in any status. Since 2026-02-16
-- the product sells one-offs through sessions in payment mode, and the purchase
-- metadata lives on the session rather than on the charge.
--
-- The connection syncs only the fields read here. Of the metadata, only the two
-- purchase keys are passed on. `updated` is the connector's cursor rather than
-- a Stripe field, and equals `created` on rows from a full sync, so it is left
-- out: the session carries no completion time.
select
    cast(id as string) as checkout_session_id,
    cast(payment_intent as string) as payment_intent_id,

    status as checkout_status,
    mode as checkout_mode,
    livemode as is_livemode,

    get_json_object(metadata, '$.paymentType') as payment_type,
    get_json_object(metadata, '$.purchaseType') as purchase_type,
    cast(amount_total / 100.0 as decimal(12, 2)) as amount_total_usd,

    timestamp_seconds(created) as created_at,

    _airbyte_extracted_at
from {{ source("airbyte_source_finance", "stripe_api_checkout_sessions") }}
