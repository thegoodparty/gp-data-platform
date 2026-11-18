-- One row per user per Win feature per day used. The single place events are
-- matched to features, so int__user_feature_use (first, last, how many days)
-- and the power user action count read the same answer.
--
-- Which events count as a feature lives in the win_feature_events seed. Staff
-- acting for the candidate is not the candidate using Win; the marker exists
-- from 2026-03-24, and earlier impersonated events cannot be told apart.
-- Rows marked win_only are agent chat events shared with Serve, so only their
-- Win use counts.
{{ config(materialized="table") }}

with
    features as (
        select
            feature,
            event_type as event_pattern,
            match_type,
            is_user_action,
            is_onboarding,
            win_only
        from {{ ref("win_feature_events") }}
    ),

    events as (
        select
            try_cast(e.user_id as bigint) as user_id,
            e.event_type,
            e.event_product,
            e.event_time
        from {{ ref("int__amplitude_events_classified") }} as e
        where
            try_cast(e.user_id as bigint) is not null
            and e.event_time >= '{{ var("product_telemetry_start") }}'
            and not coalesce(e.is_machine_emitted, false)
            and coalesce(e.event_properties:impersonation::string, 'false') != 'true'
    ),

    matched as (
        select
            e.user_id,
            f.feature,
            f.is_user_action,
            f.is_onboarding,
            date(e.event_time) as activity_date,
            e.event_time
        from events as e
        inner join
            features as f
            on (f.match_type = 'exact' and e.event_type = f.event_pattern)
            or (f.match_type = 'prefix' and startswith(e.event_type, f.event_pattern))
        where not f.win_only or e.event_product = 'win'
    )

select
    user_id,
    feature,
    activity_date,
    -- Constant per feature in the seed.
    any_value(is_user_action) as is_user_action,
    any_value(is_onboarding) as is_onboarding,
    min(event_time) as first_event_at,
    max(event_time) as last_event_at
from matched
group by user_id, feature, activity_date
