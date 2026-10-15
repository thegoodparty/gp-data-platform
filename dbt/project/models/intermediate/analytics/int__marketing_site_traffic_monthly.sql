-- Monthly traffic on the goodparty.org marketing site, from the site's own
-- Amplitude page call (Viewed, live since 2025-04-24). Product-agnostic, and
-- aggregate only: most of this traffic is anonymous, so the visitor is the
-- device rather than a person.
--
-- Single-page share is exposed rather than filtered: from early 2026 it rises
-- far above its earlier level, which reads like bot or broken-client traffic,
-- and the readers of this table should see that rather than a quietly
-- smaller count.
{{ config(materialized="table") }}

with
    page_views as (
        select
            device_id,
            -- Amplitude marks an event outside any session with -1.
            if(session_id > 0, session_id, null) as session_id,
            event_time,
            cast(date_trunc('month', event_time) as date) as month_start
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            event_type = 'Viewed'
            and device_id is not null
            -- Vercel previews, local and personal dev hosts, and the product app
            -- fire the same event.
            and lower(try_parse_url(event_properties:url::string, 'HOST'))
            in ('goodparty.org', 'www.goodparty.org')
    ),

    sessions as (
        select
            device_id,
            session_id,
            -- A session that crosses a month boundary counts in the month it began.
            cast(date_trunc('month', min(event_time)) as date) as month_start,
            count(*) as page_view_count
        from page_views
        where session_id is not null
        group by device_id, session_id
    ),

    -- A device that ever carried a user id on any event, site or product.
    identified_devices as (
        select distinct device_id
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where device_id is not null and user_id is not null
    ),

    monthly_views as (
        select
            pv.month_start,
            count(distinct pv.device_id) as visitors,
            count(*) as page_views,
            count(
                distinct case when idd.device_id is not null then pv.device_id end
            ) as visitors_identified
        from page_views as pv
        left join identified_devices as idd on idd.device_id = pv.device_id
        group by pv.month_start
    ),

    monthly_sessions as (
        select
            month_start,
            count(*) as sessions,
            count_if(page_view_count = 1) as single_page_sessions
        from sessions
        group by month_start
    )

select
    v.month_start,
    v.visitors,
    coalesce(s.sessions, 0) as sessions,
    v.page_views,
    coalesce(s.single_page_sessions, 0) as single_page_sessions,
    s.single_page_sessions / nullif(s.sessions, 0) as single_page_session_share,
    v.visitors_identified,
    v.visitors_identified / nullif(v.visitors, 0) as visitors_identified_share
from monthly_views as v
left join monthly_sessions as s on s.month_start = v.month_start
