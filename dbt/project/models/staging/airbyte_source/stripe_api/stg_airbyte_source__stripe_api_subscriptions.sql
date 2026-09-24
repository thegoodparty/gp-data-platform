-- Stripe subscriptions, one row per subscription, for the Pro plan.
--
-- Two traps this layer does not paper over. `is_deleted` marks a cancellation
-- event rather than a row to drop: it is true for 1,392 of the 2,374
-- cancelled subscriptions and false or null for the rest, so filtering on it
-- silently halves the cancelled population. And a subscription set to cancel
-- at period end is still `active` while carrying a cancellation date, so churn
-- keys on `subscription_status`, never on `canceled_at is not null`.
select
    cast(id as string) as subscription_id,
    cast(customer as string) as stripe_customer_id,
    cast(schedule as string) as schedule_id,
    cast(latest_invoice as string) as latest_invoice_id,

    status as subscription_status,
    cancel_at_period_end as will_cancel_at_period_end,
    is_deleted,
    livemode as is_livemode,
    currency,
    quantity,
    get_json_object(plan, '$.id') as plan_id,
    get_json_object(plan, '$.nickname') as plan_nickname,
    get_json_object(plan, '$.interval') as plan_interval,
    cast(
        try_cast(get_json_object(plan, '$.amount') as bigint) / 100.0 as decimal(12, 2)
    ) as plan_amount_usd,

    timestamp_seconds(created) as created_at,
    timestamp_seconds(updated) as updated_at,
    timestamp_seconds(start_date) as started_at,
    timestamp_seconds(cast(trial_start as bigint)) as trial_started_at,
    timestamp_seconds(cast(trial_end as bigint)) as trial_ends_at,
    timestamp_seconds(cast(current_period_start as bigint)) as current_period_start_at,
    timestamp_seconds(cast(current_period_end as bigint)) as current_period_end_at,
    timestamp_seconds(cast(cancel_at as bigint)) as cancel_at,
    timestamp_seconds(cast(canceled_at as bigint)) as canceled_at,
    timestamp_seconds(cast(ended_at as bigint)) as ended_at,

    _airbyte_extracted_at
from {{ source("airbyte_source", "stripe_api_subscriptions") }}
