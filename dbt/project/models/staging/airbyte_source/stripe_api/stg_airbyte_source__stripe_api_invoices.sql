-- Stripe invoices, which for this account means the recurring subscription and
-- nothing else. One-off purchases never raise one, so an invoice-only revenue
-- figure is subscription revenue rather than total revenue.
select
    cast(id as string) as invoice_id,
    cast(customer as string) as stripe_customer_id,
    cast(subscription as string) as subscription_id,
    cast(charge as string) as charge_id,
    number as invoice_number,

    status as invoice_status,
    paid as is_paid,
    billing_reason,
    is_deleted,
    livemode as is_livemode,

    currency,
    cast(total / 100.0 as decimal(12, 2)) as total_usd,
    cast(amount_due / 100.0 as decimal(12, 2)) as amount_due_usd,
    cast(amount_paid / 100.0 as decimal(12, 2)) as amount_paid_usd,
    cast(amount_remaining / 100.0 as decimal(12, 2)) as amount_remaining_usd,

    timestamp_seconds(created) as created_at,
    timestamp_seconds(updated) as updated_at,
    timestamp_seconds(effective_at) as effective_at,
    timestamp_seconds(period_start) as period_start_at,
    timestamp_seconds(period_end) as period_end_at,
    -- When the money actually arrived, as against when the invoice was
    -- raised. The two differ whenever a payment retried.
    timestamp_seconds(
        try_cast(get_json_object(status_transitions, '$.paid_at') as bigint)
    ) as paid_at,
    timestamp_seconds(
        try_cast(get_json_object(status_transitions, '$.voided_at') as bigint)
    ) as voided_at,

    _airbyte_extracted_at
from {{ source("airbyte_source", "stripe_api_invoices") }}
