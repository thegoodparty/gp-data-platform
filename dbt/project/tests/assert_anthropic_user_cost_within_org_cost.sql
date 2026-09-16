{{ config(severity="warn") }}

-- Per-user Claude cost only covers spend attributable to a seat user, so its
-- daily total can never exceed the org-wide cost report for the same day. A
-- day where it does means one side is mis-scaled (the cents-to-dollars step is
-- the historical culprit) or one stream landed a revision the other has not.
-- Compared at cent precision: the org side sums many fractional-cent rows and
-- lands a few millionths above the per-user total on days that are otherwise equal.
with
    org_daily as (
        select bucket_start, round(sum(amount), 2) as org_amount
        from {{ ref("stg_airbyte_source__anthropic_api_cost_report") }}
        group by bucket_start
    ),

    user_daily as (
        select bucket_start, round(sum(amount), 2) as user_amount
        from {{ ref("stg_airbyte_source__anthropic_api_user_cost_report") }}
        group by bucket_start
    )

-- left join so a user-cost day with no org-cost day at all is flagged too
select u.bucket_start, u.user_amount, o.org_amount
from user_daily as u
left join org_daily as o on u.bucket_start = o.bucket_start
where o.bucket_start is null or u.user_amount > o.org_amount
