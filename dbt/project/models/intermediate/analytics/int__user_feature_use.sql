-- When each user first and last used each Win feature, and on how many days.
-- One row per user per feature used.
--
-- Which events count as a feature lives in the win_feature_events seed, so
-- every analysis asking "did they use AI help" reads the same answer. Dates
-- rather than flags: each analysis applies its own cutoff (before the race,
-- first 30 days, by month) to the same rows.
--
-- Staff acting for the candidate is not the candidate using Win; the marker
-- exists from 2026-03-24, and earlier impersonated events cannot be told apart.
{{ config(materialized="table") }}

with
    features as (
        select feature, event_type as event_pattern, match_type, is_user_action
        from {{ ref("win_feature_events") }}
    ),

    events as (
        select try_cast(e.user_id as bigint) as user_id, e.event_type, e.event_time
        from {{ ref("int__amplitude_events_classified") }} as e
        where
            try_cast(e.user_id as bigint) is not null
            and e.event_time >= '{{ var("product_telemetry_start") }}'
            and not coalesce(e.is_machine_emitted, false)
            and coalesce(e.event_properties:impersonation::string, 'false') != 'true'
    ),

    matched as (
        select e.user_id, f.feature, f.is_user_action, e.event_time
        from events as e
        inner join
            features as f
            on (f.match_type = 'exact' and e.event_type = f.event_pattern)
            or (f.match_type = 'prefix' and startswith(e.event_type, f.event_pattern))
    )

select
    user_id,
    feature,
    -- Constant per feature in the seed.
    any_value(is_user_action) as is_user_action,
    min(event_time) as first_used_at,
    max(event_time) as last_used_at,
    count(distinct date(event_time)) as use_days
from matched
group by user_id, feature
