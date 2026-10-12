-- Fails for any one-off event whose row count differs from the number of
-- user_journey rows carrying that date. The events table is reshaping only, so
-- a difference means it stopped reading the column or started inventing rows.
with
    journey as (select * from {{ ref("user_journey") }}),

    expected as (
        select 'First contact' as event, count(first_touch_at) as n
        from journey
        union all
        select 'Registered', count(registered_at)
        from journey
        union all
        select 'Onboarded', count(onboarding_completed_at)
        from journey
        union all
        select 'Free product output', count(free_product_output_at)
        from journey
        union all
        select 'Activated', count(activated_at)
        from journey
        union all
        select 'First payment', count(first_payment_at)
        from journey
        union all
        select 'First subscription payment', count(first_subscription_payment_at)
        from journey
        union all
        select 'Pro', count(pro_since)
        from journey
        union all
        select 'Filing deadline', count(candidacy_filing_deadline)
        from journey
        union all
        select 'Primary election', count(candidacy_primary_election_date)
        from journey
    ),

    actual as (
        select event, count(*) as n from {{ ref("user_journey_events") }} group by event
    )

select e.event, e.n as expected_n, coalesce(a.n, 0) as actual_n
from expected as e
left join actual as a using (event)
where e.n <> coalesce(a.n, 0)
