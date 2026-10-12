-- Warns on any row; errors only past 25, so one edge-case user-month cannot
-- skip the journey mart that live Sigma workbooks read.
{{ config(warn_if=">0", error_if=">25") }}

-- Fails for any user-month with a Win or Serve activation leg that the monthly
-- table does not mark engaged. The grid only holds users with non-machine
-- activity, so an engaged month outside it would vanish without this.
with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    legs as (
        select distinct
            try_cast(e.user_id as bigint) as user_id,
            trunc(e.event_time, 'MM') as activity_month,
            {{
                is_outreach_activation_event(
                    "e.event_type",
                    "e.event_properties:method::string",
                    "e.event_properties:product::string",
                )
            }} as is_win_leg
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        where
            e.event_time >= '{{ var("product_telemetry_start") }}'
            and (
                {{
                    is_outreach_activation_event(
                        "e.event_type",
                        "e.event_properties:method::string",
                        "e.event_properties:product::string",
                    )
                }}
                or {{
                    is_serve_activation_event(
                        "e.event_type",
                        "e.event_properties:method::string",
                        "e.event_properties:product::string",
                    )
                }}
            )
    )

select l.user_id, l.activity_month, l.is_win_leg
from legs as l
join keys as k using (user_id)
left join
    {{ ref("int__user_activity_monthly") }} as m
    on m.user_id = l.user_id
    and m.activity_month = l.activity_month
where
    m.user_id is null
    or (l.is_win_leg and not m.is_engaged)
    or (not l.is_win_leg and not m.is_engaged_serve)
