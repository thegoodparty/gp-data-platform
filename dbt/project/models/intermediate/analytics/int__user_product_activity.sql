-- The product slice of the end-to-end user view: when a user first and last
-- used the product, whether they ever made anything with it, and whether they
-- were active in each of the last three months. One row per user whether or not
-- they ever appeared in Amplitude.
--
-- Machine-emitted events are excluded from every activity read here. They are
-- not a rounding error: one weekly digest broadcast, sent to users who did
-- nothing, was inflating a single month's active count by about a fifth.
--
-- Activity columns are null, not zero, for a user with no telemetry. Product
-- instrumentation starts in April 2025 and most of this table registered before
-- that or never used the product, so reading a null as a zero would report two
-- thirds of our users as inactive when we simply cannot see them.
-- has_amplitude_data is the column that tells the two apart.
--
-- Table, not view: three passes over the event stream, read by the wide journey
-- table and by its tests.
{{ config(materialized="table") }}

with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    catalog as (
        select event_type, family, is_serve, is_machine_emitted
        from {{ ref("int__amplitude_event_catalog") }}
    ),

    events as (
        select
            try_cast(e.user_id as bigint) as user_id,
            e.event_type,
            e.event_time,
            date(e.event_time) as event_date,
            date_trunc('month', e.event_time) as event_month,
            c.is_serve,
            e.event_properties:path::string as page_path,
            e.event_properties:method::string as outreach_method
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        join catalog as c on c.event_type = e.event_type
        where try_cast(e.user_id as bigint) is not null and not c.is_machine_emitted
    ),

    -- Only users on the spine. An Amplitude user id with no gp-api user behind
    -- it has nowhere to land on this row.
    user_events as (select e.* from events as e join keys as k using (user_id)),

    -- The dashboard union co-fires on a single visit, so a raw count
    -- over-counts. MIN/MAX and COUNT(DISTINCT date) are co-fire-safe; the count
    -- needs the sessionized flag.
    dashboard_view_flags as (
        select
            user_id,
            event_time,
            {{ dashboard_view_is_new("event_time", "user_id") }} as is_new_view
        from user_events
        where {{ is_dashboard_view_event("event_type", "page_path") }}
    ),

    dashboard_views as (
        select user_id, count_if(is_new_view) as dashboard_view_count
        from dashboard_view_flags
        group by user_id
    ),

    lifetime as (
        select
            user_id,
            min(event_time) as first_activity_at,
            max(event_time) as last_activity_at,
            count(*) as total_events,
            count(distinct event_date) as active_days_total,

            min(
                case
                    when {{ is_dashboard_view_event("event_type", "page_path") }}
                    then event_time
                end
            ) as first_dashboard_viewed_at,
            max(
                case
                    when {{ is_dashboard_view_event("event_type", "page_path") }}
                    then event_time
                end
            ) as last_dashboard_viewed_at,

            -- The onboarding flow's own completion event. Distinct from
            -- users_win_base.is_onboarded, which is a US-registration-plus-
            -- dashboard-view-within-14-days construction answering a different
            -- question under a similar name.
            min(
                case when event_type = 'onboarding_complete' then event_time end
            ) as onboarding_completed_at,

            min(
                case
                    when {{ is_product_output_event("event_type", "outreach_method") }}
                    then event_time
                end
            ) as product_output_at,
            min(
                case
                    when
                        {{
                            is_product_output_free_event(
                                "event_type", "outreach_method"
                            )
                        }}
                    then event_time
                end
            ) as free_product_output_at,

            min(case when is_serve then event_time end) as serve_first_activity_at,
            max(case when is_serve then event_time end) as serve_last_activity_at
        from user_events
        group by user_id
    ),

    -- Calendar months, not trailing windows, so the three flags partition time
    -- and growth_state can be read off them without overlap.
    monthly as (
        select
            user_id,
            event_month,
            count(distinct event_date) as activity_days,
            case
                when event_month = date_trunc('month', current_date())
                then 0
                when event_month = add_months(date_trunc('month', current_date()), -1)
                then 1
                when event_month = add_months(date_trunc('month', current_date()), -2)
                then 2
            end as month_offset
        from user_events
        group by user_id, event_month
    ),

    rolling as (
        select
            user_id,
            max(case when month_offset = 0 then activity_days end) as activity_days_m0,
            max(case when month_offset = 1 then activity_days end) as activity_days_m1,
            max(case when month_offset = 2 then activity_days end) as activity_days_m2,
            -- Any month older than the three-month window, which is what makes
            -- "resurrected" separable from "new".
            count_if(month_offset is null) > 0 as active_before_window
        from monthly
        group by user_id
    )

select
    k.user_id,

    l.user_id is not null as has_amplitude_data,

    l.first_activity_at,
    l.last_activity_at,
    l.total_events,
    l.active_days_total,

    dv.dashboard_view_count,
    l.first_dashboard_viewed_at,
    l.last_dashboard_viewed_at,

    l.onboarding_completed_at,
    l.onboarding_completed_at is not null as is_onboarded,

    -- Product Output: the candidate made something that left the product. A
    -- broader concept than the activation OKR, which asks only whether we
    -- reached voters for them. Never report one as the other.
    l.product_output_at,
    l.product_output_at is not null as has_product_output,
    -- The subset a non-paying user can reach. Any model predicting engagement
    -- must use this as its label; the full column is partly determined by who
    -- paid, and a model trained on it learns that instead.
    l.free_product_output_at,
    l.free_product_output_at is not null as has_free_product_output,

    l.serve_first_activity_at,
    l.serve_last_activity_at,

    coalesce(r.activity_days_m0, 0) > 0 as active_m0,
    coalesce(r.activity_days_m1, 0) > 0 as active_m1,
    coalesce(r.activity_days_m2, 0) > 0 as active_m2,
    coalesce(r.activity_days_m0, 0) as activity_days_m0,
    coalesce(r.activity_days_m1, 0) as activity_days_m1,
    coalesce(r.activity_days_m2, 0) as activity_days_m2,

    -- The bucket diagram as a column. 'dormant' is the fourth bucket's honest
    -- name: most rows are active in none of the three months, and calling them
    -- churned would claim we lost someone we may never have had.
    case
        when l.user_id is null
        then null
        when
            coalesce(r.activity_days_m0, 0) > 0
            and coalesce(r.activity_days_m1, 0) = 0
            and coalesce(r.activity_days_m2, 0) = 0
            and not coalesce(r.active_before_window, false)
        then 'new'
        when coalesce(r.activity_days_m0, 0) > 0 and coalesce(r.activity_days_m1, 0) > 0
        then 'retained'
        when coalesce(r.activity_days_m0, 0) > 0
        then 'resurrected'
        when coalesce(r.activity_days_m1, 0) > 0 or coalesce(r.activity_days_m2, 0) > 0
        then 'churned'
        else 'dormant'
    end as growth_state
from keys as k
left join lifetime as l using (user_id)
left join rolling as r using (user_id)
left join dashboard_views as dv using (user_id)
