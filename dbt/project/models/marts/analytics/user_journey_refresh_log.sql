-- One row per build of user_journey: how many rows it produced, how many
-- identities stayed unresolved, and how fresh each source was. A count that
-- moves between builds is visible here instead of only in a test run.
--
-- Append-only, and exempt from --full-refresh: the on-merge and CI jobs both
-- pass that flag, and on this model it would erase the history the model
-- exists to keep.
{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        full_refresh=false,
    )
}}

with
    journey as (
        select
            count(*) as row_count,
            count(distinct user_id) as user_count,
            count_if(gp_person_id is null) as users_without_person_id,
            count_if(not has_hubspot_contact) as users_without_hubspot_contact,
            count_if(has_candidacy) as users_with_candidacy,
            count_if(has_amplitude_data) as users_with_telemetry,
            count_if(payment_count > 0) as paying_users
        from {{ ref("user_journey") }}
    ),

    -- Paying customers who reach no user row at all, which the table above
    -- cannot show because it has no row for them.
    exceptions as (
        select count(*) as unresolved_stripe_customers
        from {{ ref("int__key_resolution_exceptions") }}
        where source_name = 'stripe'
    ),

    -- Loader time rather than event time, so a quiet day is not read as a
    -- stalled sync.
    gp_api_load as (
        select max(_airbyte_extracted_at) as gp_api_loaded_at
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    amplitude_load as (
        select max(_airbyte_extracted_at) as amplitude_loaded_at
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
    ),

    -- HubSpot contacts carry no loader column; the engagements stream rides
    -- the same connection.
    hubspot_load as (
        select max(_airbyte_extracted_at) as hubspot_loaded_at
        from {{ ref("stg_airbyte_source__hubspot_api_engagements") }}
    ),

    stripe_load as (
        select max(_airbyte_extracted_at) as stripe_loaded_at
        from {{ ref("stg_airbyte_source__stripe_api_charges") }}
    )

select
    '{{ invocation_id }}' as dbt_invocation_id, current_timestamp() as refreshed_at, *
from journey
cross join exceptions
cross join gp_api_load
cross join amplitude_load
cross join hubspot_load
cross join stripe_load
