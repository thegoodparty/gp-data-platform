{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

-- One row per requested id. The landing table is append-only, so a full_reload
-- or a genuine BallotReady change lands a second row; the newest load wins.
-- dag_run_id breaks ties because up to INSERT_BATCH_SIZE rows share a
-- loaded_at, so loaded_at alone is not a total order.
with

    {% if is_incremental() %}
        watermark as (
            -- pulled into its own CTE, cross-joined below, so the incremental
            -- filter never needs a scalar subquery in the WHERE clause.
            select coalesce(max(loaded_at), timestamp '1970-01-01') as max_loaded_at
            from {{ this }}
        ),
    {% endif %}

    current_rows as (
        select raw.*
        from {{ source("airflow_source", "ballotready_geofence_raw") }} as raw
        {% if is_incremental() %} cross join watermark {% endif %}
        where
            -- ids BallotReady returned nothing for land here with a null
            -- payload on purpose, so the landing table (not this transform) can
            -- tell "asked, got nothing" from "not fetched".
            raw.payload is not null
            {% if is_incremental() %}
                -- >= not >: a merge on requested_id is idempotent, so
                -- reprocessing the watermark boundary is free, but excluding a
                -- tied loaded_at with > would strand that row past every future
                -- run.
                and raw.loaded_at >= watermark.max_loaded_at
            {% endif %}
        qualify
            row_number() over (
                partition by raw.requested_id
                order by raw.loaded_at desc, raw.dag_run_id desc
            )
            = 1
    )

select
    requested_id,
    loaded_at,
    cast(get_json_object(payload, '$.createdAt') as timestamp) as created_at,
    -- int, not bigint: the Python model this replaces emits IntegerType and downstream
    -- marts are built against that. Max observed id is 1,528,079.
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    get_json_object(payload, '$.geoId') as geo_id,
    get_json_object(payload, '$.id') as id,
    get_json_object(payload, '$.mtfcc') as mtfcc,
    cast(get_json_object(payload, '$.updatedAt') as timestamp) as updated_at,
    cast(get_json_object(payload, '$.validFrom') as date) as valid_from,
    cast(get_json_object(payload, '$.validTo') as date) as valid_to
from current_rows
