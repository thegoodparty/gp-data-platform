{{ config(materialized="table") }}

/*
    int__amplitude_event_taxonomy

    Purpose:
        Single source of truth for Amplitude event-family classification.
        One row per distinct event_type, classified into a product-feature
        family via pattern matching. Replaces event-type logic that was
        duplicated across int__amplitude_win_activity, int__amplitude_user_milestones,
        the analytics helper, and the runbook (DATA-1945, calibration finding C1).

    Grain:
        One row per distinct event_type seen in the raw event stream.

    Source:
        {{ ref('stg_airbyte_source__amplitude_api_events') }}

    Columns:
        - event_type      : distinct Amplitude event name (primary key)
        - family          : product-feature bucket (see amplitude_event_family macro)
        - is_win          : Win-product candidate-attributed family (family like 'win_%')
        - is_recurrent    : recurrent activity vs one-off lifecycle milestone
                            (see amplitude_event_is_recurrent macro)
        - first_seen_date : first appearance in the raw stream; enables drift control
                            via WHERE first_seen_date <= '<cutoff>'
        - event_count     : total occurrences (volume context for drift triage)

    Notes:
        - Classification is pattern-based, so new event_types in known families
          classify automatically (e.g. the ~2026-05-06 onboarding redesign events).
        - Materialized as a table (not a view like sibling int__amplitude models)
          because it is a full-stream aggregation feeding many consumers, including
          notebooks that SELECT/JOIN it directly via databricks-sql-connector and
          cannot call dbt macros. It is tiny (~one row per event_type).
        - The _list catalog (stg_airbyte_source__amplitude_api_events_list) is used
          only as a dev-time coverage cross-check, not unioned into the grain, so we
          do not classify catalog-defined events that never fired.
*/
with
    event_stream as (
        select
            event_type,
            min(cast(event_time as date)) as first_seen_date,
            count(*) as event_count
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where event_type is not null
        group by event_type
    )

select
    event_type,
    {{ amplitude_event_family("event_type") }} as family,
    family like 'win_%' as is_win,
    {{ amplitude_event_is_recurrent("event_type") }} as is_recurrent,
    first_seen_date,
    event_count
from event_stream
