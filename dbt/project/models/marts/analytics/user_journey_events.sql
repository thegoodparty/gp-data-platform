-- Every dated moment on each user's journey, one row per moment. Internal only.
--
-- Reshaping only. One-off moments are user_journey columns; touches and sends
-- are the histories user_journey rolls up. A moment with no date has no row,
-- because the live timeline chart reads this table and a null date would
-- become its own bucket.
--
-- Elections: the primary when the candidacy had one, then the general unless
-- the civics record says the run ended at the primary. latest_stage_reached
-- decides that, not the primary result, because the two disagree for some
-- candidacies. Without a civics candidacy the anchor date stands in for the
-- general; it is the general for nearly every matched run.
with
    journey as (
        select
            user_id,
            product,
            registered_at,
            first_touch_at,
            onboarding_completed_at,
            free_product_output_at,
            activated_at,
            first_payment_at,
            first_subscription_payment_at,
            pro_since,
            candidacy_filing_deadline,
            candidacy_primary_election_date,
            candidacy_primary_result,
            coalesce(
                candidacy_general_election_date, candidacy_election_date
            ) as general_election_date,
            candidacy_general_result,
            coalesce(
                candidacy_latest_stage_reached like 'primary%', false
            ) as ended_at_primary
        from {{ ref("user_journey") }}
    ),

    one_off as (
        select
            user_id,
            'First contact' as event,
            'acquisition' as event_category,
            cast(null as string) as stage,
            9 as event_order,
            first_touch_at as event_at,
            cast(null as string) as event_label
        from journey
        union all
        select user_id, 'Registered', 'milestone', 'registered', 1, registered_at, null
        from journey
        union all
        select
            user_id,
            'Onboarded',
            'milestone',
            'completed_onboarding',
            2,
            onboarding_completed_at,
            null
        from journey
        union all
        select
            user_id,
            'Free product output',
            'milestone',
            null,
            3,
            free_product_output_at,
            null
        from journey
        union all
        select user_id, 'Activated', 'milestone', 'activated', 6, activated_at, null
        from journey
        union all
        select user_id, 'First payment', 'revenue', null, 4, first_payment_at, null
        from journey
        union all
        select
            user_id,
            'First subscription payment',
            'revenue',
            null,
            12,
            first_subscription_payment_at,
            null
        from journey
        union all
        select user_id, 'Pro', 'revenue', 'pro', 5, pro_since, null
        from journey
        union all
        select
            user_id,
            'Filing deadline',
            'election',
            null,
            13,
            cast(candidacy_filing_deadline as timestamp),
            null
        from journey
        union all
        select
            user_id,
            'Primary election',
            'election',
            null,
            7,
            cast(candidacy_primary_election_date as timestamp),
            candidacy_primary_result
        from journey
        union all
        select
            user_id,
            'General election',
            'election',
            null,
            8,
            cast(general_election_date as timestamp),
            candidacy_general_result
        from journey
        where not ended_at_primary
    ),

    -- Outbound only: contact we made. Notes, tasks and inbound mail stay off,
    -- matching the per-user touch counts.
    touches as (
        select
            t.user_id,
            'HubSpot touch' as event,
            'touch' as event_category,
            cast(null as string) as stage,
            10 as event_order,
            'touch:' || t.touch_id as event_key,
            t.touch_at as event_at,
            t.channel as event_label,
            cast(null as bigint) as event_value
        from {{ ref("int__user_hubspot_touches") }} as t
        where t.is_outbound_touch
    ),

    -- Committed only: a requested send never confirmed delivery, and the row's
    -- voters-reached figure counts committed sends.
    sends as (
        select
            s.user_id,
            'Outreach send' as event,
            'send' as event_category,
            cast(null as string) as stage,
            11 as event_order,
            'send:' || s.send_key as event_key,
            s.send_at as event_at,
            s.channel as event_label,
            cast(s.recipient_count as bigint) as event_value
        from {{ ref("int__user_outreach_sends") }} as s
        where s.basis = 'committed'
    ),

    events as (
        select
            user_id,
            event,
            event_category,
            stage,
            event_order,
            event as event_key,
            event_at,
            event_label,
            cast(null as bigint) as event_value
        from one_off
        union all
        select *
        from touches
        union all
        select *
        from sends
    )

select
    e.user_id,
    e.event,
    e.event_category,
    e.stage,
    e.event_order,
    e.event_key,
    e.event_at,
    e.event_label,
    e.event_value,
    j.product,
    datediff(e.event_at, j.registered_at) as days_since_registered
from events as e
join journey as j using (user_id)
where e.event_at is not null
