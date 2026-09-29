-- Every payment attempt against the Stripe account, successful or not. This is
-- the only stream that sees all the money: invoices cover the subscription
-- alone, while one-off purchases (domains, outreach, sales-led checkout) reach
-- Stripe as a bare charge and never produce one.
--
-- The connector lands times as epoch seconds and amounts as integer cents.
-- Both are converted here so no consumer has to remember to.
select
    cast(id as string) as charge_id,
    cast(customer as string) as stripe_customer_id,
    cast(invoice as string) as invoice_id,
    cast(payment_intent as string) as payment_intent_id,
    cast(balance_transaction as string) as balance_transaction_id,

    status as charge_status,
    paid as is_paid,
    captured as is_captured,
    refunded as is_fully_refunded,
    disputed as is_disputed,
    livemode as is_livemode,

    currency,
    cast(amount / 100.0 as decimal(12, 2)) as amount_usd,
    cast(amount_captured / 100.0 as decimal(12, 2)) as amount_captured_usd,
    cast(amount_refunded / 100.0 as decimal(12, 2)) as amount_refunded_usd,

    timestamp_seconds(created) as created_at,
    timestamp_seconds(updated) as updated_at,

    description,
    receipt_email,
    failure_code,
    failure_message,
    get_json_object(payment_method_details, '$.type') as payment_method_type,

    -- Metadata our own product writes at checkout. It stopped arriving on
    -- 2026-02-16, when checkout moved behind a payment intent that this
    -- connector does not ingest, so every key below is null for later
    -- charges and the purchase line becomes unclassifiable. Dollars are
    -- unaffected: the customer id still lands.
    get_json_object(metadata, '$.paymentType') as payment_type,
    get_json_object(metadata, '$.purchaseType') as purchase_type,
    try_cast(get_json_object(metadata, '$.userId') as bigint) as product_user_id,
    try_cast(
        get_json_object(metadata, '$.campaignId') as bigint
    ) as product_campaign_id,
    try_cast(get_json_object(metadata, '$.websiteId') as bigint) as product_website_id,
    get_json_object(metadata, '$.domainName') as domain_name,
    get_json_object(metadata, '$.outreachType') as outreach_type,
    try_cast(
        get_json_object(metadata, '$.contactCount') as bigint
    ) as outreach_contact_count,

    -- Purchases taken through HubSpot commerce rather than the product. These
    -- keep their metadata throughout.
    get_json_object(metadata, '$.checkoutSessionId') as hs_checkout_session_id,
    get_json_object(metadata, '$.portalId') as hs_portal_id,

    _airbyte_extracted_at
from {{ source("airbyte_source", "stripe_api_charges") }}
