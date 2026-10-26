{{ config(severity="warn", store_failures=true, schema="mart_finance") }}

-- Pro status lives in two places that drift apart: Stripe, which bills, and
-- the campaign row, which grants Pro and holds the only pointer to the
-- subscription (details.subscriptionId). Each row is a live Pro subscription
-- on one side that the other side does not account for, so someone is billed
-- for nothing, billed twice, or cannot cancel. The classes mirror omni's
-- stripe-campaign-reconcile.ts, plus subscriptions on non-Pro campaigns, which
-- that script skips.
--
-- Warn, not error: a failing test in dbt build skips every descendant of the
-- models it reads, and rows clear only when CS acts in Stripe. The failures
-- are stored in mart_finance, not the shared audit schema, for weekly triage.
--
-- Not tested: Pro campaigns with no live subscription. Comped and legacy Pro
-- look identical to it in the schema, so it would never go quiet.
--
-- Blind spot: the campaign stream does not capture hard deletes, so a
-- subscription whose campaign was deleted still reads as carried here.
with
    pro_subscriptions as (
        select
            s.subscription_id,
            s.stripe_customer_id,
            s.subscription_status,
            s.current_period_end_at,
            -- Stripe never bills these again.
            s.subscription_status
            not in ('canceled', 'incomplete_expired') as is_uncanceled,
            -- Collecting now, or one payment retry from it.
            s.subscription_status in ('active', 'trialing', 'past_due') as is_live
        from {{ ref("stg_airbyte_source_finance__stripe_api_subscriptions") }} as s
        inner join
            {{ ref("stg_airbyte_source_finance__stripe_api_plans") }} as p
            on p.id = s.plan_id
        where s.is_livemode and p.product = '{{ var("stripe_pro_product_id") }}'
    ),

    -- Stripe lands once a day and campaigns hourly. A campaign that changed
    -- after the Stripe snapshot cannot be compared to it yet: a subscription
    -- ending at period end de-Pros its campaign hours before Stripe shows it
    -- canceled.
    stripe_snapshot as (
        select max(_airbyte_extracted_at) as synced_at
        from {{ ref("stg_airbyte_source_finance__stripe_api_subscriptions") }}
    ),

    campaign_links as (
        select
            id as campaign_id,
            user_id,
            is_pro,
            updated_at,
            details:subscriptionid::string as subscription_id
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
        where details:subscriptionid::string is not null
    ),

    -- An email-only checkout minted a new Stripe customer per session, so one
    -- person can hold Pro twice across customer records. Email is the person
    -- key where Stripe has one; a customer with none still groups with itself.
    person_keys as (
        select
            s.subscription_id,
            coalesce(
                nullif(lower(trim(c.email)), ''), s.stripe_customer_id
            ) as person_key
        from pro_subscriptions as s
        left join
            {{ ref("stg_airbyte_source_finance__stripe_api_customers") }} as c
            on c.id = s.stripe_customer_id
        where s.is_uncanceled
    ),

    duplicate_groups as (
        select
            person_key,
            array_sort(collect_list(subscription_id)) as related_subscription_ids
        from person_keys
        group by person_key
        having count(*) > 1
    ),

    duplicates as (
        select k.subscription_id, g.related_subscription_ids
        from person_keys as k
        inner join duplicate_groups as g using (person_key)
    ),

    linked_subscriptions as (
        select
            subscription_id,
            bool_or(coalesce(is_pro, false)) as is_any_campaign_pro,
            max(updated_at) as campaign_updated_at,
            min(campaign_id) as campaign_id
        from campaign_links
        group by subscription_id
    ),

    mismatched_owners as (
        select
            s.subscription_id,
            l.campaign_id,
            l.user_id,
            u.stripe_customer_id as owner_stripe_customer_id
        from pro_subscriptions as s
        inner join campaign_links as l using (subscription_id)
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_user") }} as u on u.id = l.user_id
        where s.is_live and u.stripe_customer_id != s.stripe_customer_id
    ),

    drift as (
        select
            'duplicate_pro_subscription' as drift_class,
            d.subscription_id,
            cast(null as bigint) as campaign_id,
            cast(null as bigint) as user_id,
            cast(null as string) as owner_stripe_customer_id,
            d.related_subscription_ids
        from duplicates as d

        union all

        -- Billing with no campaign carrying the id, so nobody can see or
        -- cancel it in the product. A duplicate group already reports its
        -- unlinked members.
        select 'orphaned_live_subscription', s.subscription_id, null, null, null, null
        from pro_subscriptions as s
        left anti join campaign_links as l using (subscription_id)
        left anti join duplicates as d using (subscription_id)
        where s.is_live

        union all

        select
            'live_subscription_on_non_pro_campaign',
            s.subscription_id,
            l.campaign_id,
            null,
            null,
            null
        from pro_subscriptions as s
        inner join linked_subscriptions as l using (subscription_id)
        cross join stripe_snapshot as snap
        where
            s.is_live
            and not l.is_any_campaign_pro
            and l.campaign_updated_at <= snap.synced_at

        union all

        -- Manage Subscription opens the owner's stored customer, not the one
        -- being billed, so the owner cannot cancel it. A missing stored
        -- customer is not drift: the portal recovers it from the subscription.
        select
            'owner_stripe_customer_mismatch',
            m.subscription_id,
            m.campaign_id,
            m.user_id,
            m.owner_stripe_customer_id,
            null
        from mismatched_owners as m
    )

select
    drift.drift_class,
    drift.subscription_id,
    s.subscription_status,
    s.stripe_customer_id,
    s.current_period_end_at,
    drift.campaign_id,
    drift.user_id,
    drift.owner_stripe_customer_id,
    drift.related_subscription_ids
from drift
inner join pro_subscriptions as s using (subscription_id)
