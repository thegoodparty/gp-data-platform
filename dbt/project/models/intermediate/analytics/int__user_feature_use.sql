-- When each user first and last used each Win feature, and on how many days.
-- One row per user per feature used.
--
-- Which events count as a feature lives in the win_feature_events seed and is
-- matched once, in int__user_feature_days, so every analysis asking "did they
-- use AI help" reads the same answer. Dates rather than flags: each analysis
-- applies its own cutoff (before the race, first 30 days, by month) to the
-- same rows.
{{ config(materialized="table") }}

select
    user_id,
    feature,
    -- Constant per feature in the seed.
    any_value(is_user_action) as is_user_action,
    min(first_event_at) as first_used_at,
    max(last_event_at) as last_used_at,
    count(*) as use_days
from {{ ref("int__user_feature_days") }}
group by user_id, feature
