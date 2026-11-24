{{ config(severity="warn") }}

-- The Anthropic arm's daily total is the org report by construction: the seat
-- rows plus an `unattributed` row holding whatever the seats did not
-- explain. A day where the two disagree means a dropped or doubled residual, or
-- a dropped or doubled seat row in the model's output, or one report landed a
-- revision the other has not.
--
-- A day the org report covers with an arm total of nothing is not a failure: a
-- zero-cost day emits no rows at all, by design. So a missing arm total counts
-- as zero on an org-backed day, and the warning is kept for a day the arm has
-- spend on and the org report does not cover.
--
-- Compared at cent precision because the org side sums many fractional-cent
-- rows. Warn, not error: revision skew is a reachable state the repo already
-- tolerates in assert_anthropic_user_cost_within_org_cost, and blocking a build
-- on it would fail for a reason no code change can fix.
with
    arm_daily as (
        select usage_date, round(sum(cost_usd), 2) as arm_cost_usd
        from {{ ref("ai_usage_daily") }}
        where source = 'anthropic'
        group by usage_date
    ),

    org_daily as (
        select
            to_date(bucket_start) as usage_date, round(sum(amount), 2) as org_cost_usd
        from {{ ref("stg_airbyte_source__anthropic_api_cost_report") }}
        group by 1
    )

select
    coalesce(arm_daily.usage_date, org_daily.usage_date) as usage_date,
    arm_daily.arm_cost_usd,
    org_daily.org_cost_usd
from arm_daily
full outer join org_daily on arm_daily.usage_date = org_daily.usage_date
where
    org_daily.org_cost_usd is null
    or coalesce(arm_daily.arm_cost_usd, 0) != org_daily.org_cost_usd
