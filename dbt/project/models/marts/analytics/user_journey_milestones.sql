-- The journey stages from user_journey, one row per user per stage reached, so
-- a chart can count users by stage. Internal only.
--
-- Reshaping only. Every stage reads a flag the row already defines, so this
-- table cannot disagree with it. Filter by joining to user_journey on user_id.
--
-- Stages are not nested: a user can be Pro without finishing onboarding. Read a
-- stage as "share of users who reached it", never as drop-off from the one
-- before. reached_at is null where the stage has no date: pro_since for about
-- half of Pro users, and every third-party match.
with
    journey as (
        select
            user_id,
            registered_at,
            is_onboarded,
            onboarding_completed_at,
            is_activated,
            activated_at,
            is_pro,
            pro_since,
            candidacy_has_external_match
        from {{ ref("user_journey") }}
    ),

    stages as (
        select
            user_id,
            'registered' as stage,
            1 as stage_order,
            registered_at as reached_at
        from journey
        union all
        select user_id, 'completed_onboarding', 2, onboarding_completed_at
        from journey
        where is_onboarded
        union all
        select user_id, 'activated', 3, activated_at
        from journey
        where is_activated
        union all
        select user_id, 'pro', 4, pro_since
        from journey
        where is_pro
        union all
        select user_id, 'matched_third_party', 5, cast(null as timestamp)
        from journey
        where candidacy_has_external_match
    )

select user_id, stage, stage_order, reached_at
from stages
