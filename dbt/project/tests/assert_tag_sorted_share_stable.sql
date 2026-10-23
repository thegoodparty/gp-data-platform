{{ config(severity="warn") }}

-- A tag edit in Govern can move users between "active" and "not" overnight.
-- Warns when the share of a product's active users who rely on a tag or
-- profile (any event not sorted by name) moves more than 0.10 month to month.
-- Counts distinct logged-in users with a non-machine event in the month, minus
-- staff and test accounts (the person-keyed int__civics_internal_persons set).
with
    internal_users as (
        select k.user_id
        from {{ ref("int__user_resolved_keys") }} as k
        inner join
            {{ ref("int__civics_internal_persons") }} as i
            on i.gp_person_id = k.gp_person_id
    ),

    months as (
        select
            add_months(trunc(current_date(), 'MM'), -2) as prior_month,
            add_months(trunc(current_date(), 'MM'), -1) as latest_month
    ),

    shares as (
        select
            e.event_product,
            trunc(e.event_time, 'MM') as activity_month,
            count(distinct try_cast(e.user_id as bigint)) as active_users,
            count(
                distinct case
                    when e.product_source != 'event_name'
                    then try_cast(e.user_id as bigint)
                end
            ) as tag_sorted_users
        from {{ ref("int__amplitude_events_classified") }} as e
        cross join months as m
        left anti join internal_users as i on i.user_id = try_cast(e.user_id as bigint)
        where
            e.event_product in ('win', 'serve')
            and try_cast(e.user_id as bigint) is not null
            and not e.is_machine_emitted
            and e.event_time >= '{{ var("product_telemetry_start") }}'
            and trunc(e.event_time, 'MM') in (m.prior_month, m.latest_month)
        group by 1, 2
    )

select
    latest.event_product,
    prior.tag_sorted_users / prior.active_users as prior_share,
    latest.tag_sorted_users / latest.active_users as latest_share
from shares as latest
cross join months as m
join
    shares as prior
    on prior.event_product = latest.event_product
    and prior.activity_month = m.prior_month
where
    latest.activity_month = m.latest_month
    and abs(
        latest.tag_sorted_users / latest.active_users
        - prior.tag_sorted_users / prior.active_users
    )
    > 0.10
