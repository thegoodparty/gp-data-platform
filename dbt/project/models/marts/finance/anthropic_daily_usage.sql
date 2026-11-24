-- Anthropic spend, one row per UTC day and person, plus one `unattributed` row
-- per day carrying everything the seat report cannot attribute.
--
-- The daily total therefore equals the org cost report's by construction. Do not
-- read this beside anthropic_analytics_org_cost_report: the residual row already
-- carries that difference and reading both double counts it.
--
-- Amounts stay decimal end to end. A double anywhere in the subtraction leaves
-- millionth-of-a-dollar noise that puts the residual on the wrong side of zero.
with

    org_daily as (

        select
            to_date(bucket_start) as usage_date, round(sum(amount), 2) as org_cost_usd
        from {{ ref("stg_airbyte_source__anthropic_api_cost_report") }}
        group by 1

    ),

    org_loaded as (

        select max(_airbyte_extracted_at) as loaded_at
        from {{ ref("stg_airbyte_source__anthropic_api_cost_report") }}

    ),

    seat_loaded as (

        select max(_airbyte_extracted_at) as loaded_at
        from {{ ref("stg_airbyte_source__anthropic_api_user_cost_report") }}

    ),

    -- The arm has one read time, taken as the earlier of the two reports: a day
    -- where only one of them was re-read sits on a stale half, and a per-row
    -- stamp would report the day's own load rather than whether the feed is
    -- still running. Null when either report has never loaded: least() skips a
    -- null argument rather than propagating it, so a bare least() would let an
    -- empty seat report inherit the org report's stamp and claim an anchor the
    -- arm has not earned.
    arm_anchor as (

        select
            case
                when org_loaded.loaded_at is null or seat_loaded.loaded_at is null
                then null
                else least(org_loaded.loaded_at, seat_loaded.loaded_at)
            end as source_loaded_at
        from org_loaded
        cross join seat_loaded

    ),

    -- A day is complete once both reports were read after that day closed. Not
    -- the newest bucket: the sync lands mid-day, so the newest bucket is partial.
    complete_through as (

        select max(org_daily.usage_date) as source_complete_through
        from org_daily
        cross join arm_anchor
        where org_daily.usage_date < to_date(arm_anchor.source_loaded_at)

    ),

    seat_daily as (

        select
            to_date(bucket_start) as usage_date,
            lower(user_email) as person_email,
            sum(amount) as cost_usd,
            sum(requests) as request_count
        from {{ ref("stg_airbyte_source__anthropic_api_user_cost_report") }}
        -- A deleted seat carries a null email. Its spend is still real, so it
        -- falls into the residual row below instead of failing attribution.
        where user_email is not null
        group by 1, 2

    ),

    seat_totals as (

        select usage_date, round(sum(cost_usd), 2) as seat_cost_usd
        from seat_daily
        group by 1

    ),

    -- Driven from the org totals with the seat side defaulting to zero, so a day
    -- with no seat rows still gets its row. Both sides are already at cents, and
    -- a day whose difference is exactly zero has nothing left to attribute, so
    -- those rows are dropped where this is read.
    residual as (

        select
            org_daily.usage_date,
            round(
                org_daily.org_cost_usd - coalesce(seat_totals.seat_cost_usd, 0), 2
            ) as cost_usd
        from org_daily
        left join seat_totals on org_daily.usage_date = seat_totals.usage_date

    ),

    all_rows as (

        select
            usage_date,
            person_email,
            false as is_unattributed,
            cast(cost_usd as decimal(20, 6)) as cost_usd,
            cast(request_count as bigint) as request_count
        from seat_daily

        union all

        select
            usage_date,
            'unattributed' as person_email,
            true as is_unattributed,
            cast(cost_usd as decimal(20, 6)) as cost_usd,
            -- Null, not zero: the org report leaves requests null on every row,
            -- so the residual's request count is not computable and a zero would
            -- be a claim a sum would hide.
            cast(null as bigint) as request_count
        from residual
        where cost_usd <> 0

    )

select
    all_rows.usage_date,
    all_rows.person_email,
    all_rows.is_unattributed,
    all_rows.cost_usd,
    all_rows.request_count,
    arm_anchor.source_loaded_at,
    complete_through.source_complete_through
from all_rows
cross join arm_anchor
cross join complete_through
