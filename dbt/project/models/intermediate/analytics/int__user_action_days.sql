-- One row per user per day on which they took at least one real action in Win,
-- with the features they used that day. The daily grain power user v2 counts.
--
-- A real action is a user action in the win_feature_events seed that is not an
-- onboarding step, or a send. Sends come from the is_activated legs rather than
-- the seed, so "sent" here and activation cannot drift apart. Dashboard views
-- are not actions: looking is not campaigning.
--
-- The feature leg drops machine-emitted and impersonated events
-- (int__user_feature_days). The send leg keeps them, as is_activated does.
{{ config(materialized="table") }}

with
    feature_days as (
        select user_id, activity_date, feature
        from {{ ref("int__user_feature_days") }}
        where is_user_action and not is_onboarding
    ),

    send_days as (
        select user_id, activity_date, 'outreach_send' as feature
        from {{ ref("int__user_engagement_days") }}
        where has_activation_event
    ),

    action_days as (
        select user_id, activity_date, feature
        from feature_days
        union
        select user_id, activity_date, feature
        from send_days
    )

select
    user_id,
    activity_date,
    array_sort(collect_set(feature)) as action_features,
    count(distinct feature) as action_feature_count,
    bool_or(feature = 'outreach_send') as has_send
from action_days
group by user_id, activity_date
