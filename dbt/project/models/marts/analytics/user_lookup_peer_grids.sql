-- Per-user inputs to the User lookup page's peer medians, with the zeros filled
-- in. A peer who never touched an area, skipped a month or had not sent by a
-- week still counts in the median, and Sigma has no cross join to add those
-- rows itself. The peer group is picked on the page; this table only holds
-- each user's own numbers. Internal only.
--
-- kind = area: share of the user's active days that touched each product area.
-- kind = month: active days in each month, counted back from the election
-- month on Win (step = months before) and forward from the Serve clock on
-- Serve (step = months since).
-- kind = week: outreach sends to date at each week, back from election day on
-- Win (step = weeks before, sends up to 52 weeks out) and forward from the
-- Serve clock on Serve (step = weeks since).
--
-- Area and month rows cover users with product activity from June 2025, when
-- the page's Amplitude window starts; months before it are left out, not zero.
-- Month and week rows stop at today.
with
    users as (
        select
            u.user_id,
            u.registered_at,
            u.serve_started_at,
            u.candidacy_election_date,
            u.product in ('win', 'both') as is_win,
            coalesce(u.is_serve_user, false) as is_serve,
            c.anchor_at
        from {{ ref("user_journey") }} as u
        left join {{ ref("user_serve_clock") }} as c using (user_id)
    ),

    area_months as (
        select *
        from {{ ref("user_product_area_activity_monthly") }}
        where activity_month >= date '2025-06-01'
    ),

    -- product_active_days repeats on each area row of a month; take it once.
    product_months as (
        select distinct user_id, product, activity_month, product_active_days
        from area_months
    ),

    product_days as (
        select user_id, product, sum(product_active_days) as product_active_days
        from product_months
        group by 1, 2
    ),

    area_days as (
        select user_id, product, product_area, sum(area_active_days) as area_days
        from area_months
        group by 1, 2, 3
    ),

    areas as (select distinct product, product_area from area_days),

    area_grid as (
        select
            p.user_id,
            p.product,
            'area' as kind,
            a.product_area,
            cast(null as int) as step,
            coalesce(d.area_days, 0) as active_days,
            p.product_active_days,
            coalesce(d.area_days, 0) / p.product_active_days as day_share,
            cast(null as bigint) as sends,
            cast(null as bigint) as voters,
            cast(null as boolean) as is_partial_month
        from product_days as p
        join areas as a using (product)
        left join area_days as d using (user_id, product, product_area)
    ),

    month_offsets as (select explode(sequence(0, 24)) as step),

    week_offsets as (select explode(sequence(0, 52)) as step),

    products as (
        select 'win' as product
        union all
        select 'serve'
    ),

    month_steps as (
        select
            p.user_id,
            p.product,
            s.step,
            case
                when p.product = 'serve'
                then add_months(trunc(u.anchor_at, 'MM'), s.step)
                else add_months(trunc(u.candidacy_election_date, 'MM'), - s.step)
            end as month_start,
            trunc(
                case
                    when p.product = 'serve'
                    then u.serve_started_at
                    else u.registered_at
                end,
                'MM'
            ) as start_month
        from product_days as p
        join users as u using (user_id)
        cross join month_offsets as s
        where
            case
                when p.product = 'serve'
                then u.is_serve and u.anchor_at is not null
                else u.is_win and u.candidacy_election_date is not null and s.step <= 12
            end
    ),

    month_grid as (
        select
            s.user_id,
            s.product,
            'month' as kind,
            cast(null as string) as product_area,
            s.step,
            coalesce(
                case
                    when s.product = 'serve'
                    then g.serve_activity_days
                    else g.win_activity_days
                end,
                0
            ) as active_days,
            cast(null as bigint) as product_active_days,
            cast(null as double) as day_share,
            cast(null as bigint) as sends,
            cast(null as bigint) as voters,
            s.month_start = trunc(current_date(), 'MM') as is_partial_month
        from month_steps as s
        left join
            {{ ref("user_growth_state_monthly") }} as g
            on g.user_id = s.user_id
            and g.activity_month = s.month_start
        where
            s.month_start between s.start_month and current_date()
            and s.month_start >= date '2025-06-01'
    ),

    sends as (
        select
            u.user_id,
            case when e.event = 'Serve outreach' then 'serve' else 'win' end as product,
            coalesce(e.event_value, 0) as voters,
            case
                when e.event = 'Serve outreach'
                then floor(datediff(e.event_at, u.anchor_at) / 7)
                else floor(datediff(u.candidacy_election_date, e.event_at) / 7)
            end as week
        from {{ ref("user_journey_events") }} as e
        join users as u using (user_id)
        where e.event in ('Outreach send', 'Serve outreach')
    ),

    week_steps as (
        select u.user_id, p.product, s.step
        from users as u
        cross join products as p
        cross join week_offsets as s
        where
            case
                when p.product = 'serve'
                then
                    u.is_serve
                    and u.anchor_at is not null
                    and date_add(to_date(u.anchor_at), s.step * 7) <= current_date()
                else
                    u.is_win
                    and u.candidacy_election_date is not null
                    and s.step <= 26
                    and date_sub(u.candidacy_election_date, s.step * 7)
                    <= current_date()
            end
    ),

    -- Serve counts sends from the clock to the week; Win counts sends from 52
    -- weeks out to the week before election day.
    week_sends as (
        select
            s.user_id,
            s.product,
            s.step,
            x.voters,
            case
                when s.product = 'serve'
                then x.week between 0 and s.step
                else x.week between s.step and 52
            end as in_window
        from week_steps as s
        left join sends as x using (user_id, product)
    ),

    week_grid as (
        select
            x.user_id,
            x.product,
            'week' as kind,
            cast(null as string) as product_area,
            x.step,
            cast(null as bigint) as active_days,
            cast(null as bigint) as product_active_days,
            cast(null as double) as day_share,
            count_if(x.in_window) as sends,
            coalesce(sum(case when x.in_window then x.voters end), 0) as voters,
            cast(null as boolean) as is_partial_month
        from week_sends as x
        group by x.user_id, x.product, x.step
    )

select *
from area_grid
union all
select *
from month_grid
union all
select *
from week_grid
