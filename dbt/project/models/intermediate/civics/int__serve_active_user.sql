-- int__serve_active_user: the single source of truth for the
-- "active serve user" BEHAVIORAL definition, one row per user. A user is an
-- active serve user when they have reached constituents through the product, by
-- any leg activated_serve_users declares, AND have pledged. This is the behavioral
-- half of the People Served cohort; the
-- office half (Serve-ICP) and the internal-email exclusion are applied downstream
-- at int__serve_district_resolution, where the office and the owner email live,
-- so this model stays a pure user-grain behavioral table.
--
-- Reads staging + intermediate ONLY, never an analytics mart: the cohort flag it
-- feeds is consumed by int__serve_block_coverage, which sits below the analytics
-- layer, so sourcing from a mart would risk a dependency cycle.
--
-- "Pledged" is "any pledged campaign" from source-staging campaigns. On the serve
-- cohort this is identical to the stricter "latest candidacy pledged" notion
-- (proven: zero membership delta), so the simpler source-staging read is canonical.
with
    activation as (
        -- int__amplitude_user_milestones is already one non-null row per user_id
        select
            user_id,
            (first_sms_poll_sent_at is not null) as has_sent_sms_poll,
            first_serve_activated_at,
            (first_serve_activated_at is not null) as has_reached_constituents
        from {{ ref("int__amplitude_user_milestones") }}
    ),

    pledged as (
        select
            user_id, bool_or(coalesce(details:pledged::boolean, false)) as has_pledged
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
        where user_id is not null
        group by user_id
    ),

    combined as (
        select
            coalesce(a.user_id, p.user_id) as user_id,
            coalesce(a.has_sent_sms_poll, false) as has_sent_sms_poll,
            a.first_serve_activated_at,
            coalesce(a.has_reached_constituents, false) as has_reached_constituents,
            coalesce(p.has_pledged, false) as has_pledged
        from activation a
        full outer join pledged p on a.user_id = p.user_id
    )

select
    user_id,
    has_sent_sms_poll,
    first_serve_activated_at,
    has_reached_constituents,
    has_pledged,
    has_reached_constituents and has_pledged as is_active_serve_user
from combined
