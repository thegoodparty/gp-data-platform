{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

-- One row per requested id. The landing table is append-only, so a full_reload or a
-- genuine BallotReady change lands a second row; the newest load wins. dag_run_id
-- breaks
-- ties because up to INSERT_BATCH_SIZE rows share a loaded_at, so loaded_at alone is
-- not
-- a total order.
with
    current_rows as (
        select *
        from {{ source("airflow_source", "ballotready_geofence_raw") }}
        where
            payload is not null
            -- >= not >: a merge on requested_id is idempotent, so reprocessing the
            -- watermark boundary is free, but excluding a tied loaded_at with > would
            -- strand that row past every future run.
            {% if is_incremental() %}
                and loaded_at >= (
                    select coalesce(max(loaded_at), timestamp '1970-01-01')
                    from {{ this }}
                )
            {% endif %}
        qualify
            row_number() over (
                partition by requested_id order by loaded_at desc, dag_run_id desc
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
