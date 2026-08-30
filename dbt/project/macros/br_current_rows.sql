{% macro br_current_rows(raw_table, preserve_created_at=false) %}
    -- One row per requested id. The landing table is append-only, so a full_reload or a
    -- genuine BallotReady change lands a second row; the newest load wins. dag_run_id
    -- breaks ties for the case that matters: the same requested_id landing twice
    -- under a
    -- shared loaded_at across separate inserts in one run, such as a retried window.
    with

        {% if is_incremental() %}
            watermark as (
                -- own CTE, cross-joined below, so the incremental filter never needs a
                -- scalar subquery in the WHERE clause.
                select coalesce(max(loaded_at), timestamp '1970-01-01') as max_loaded_at
                from {{ this }}
            ),
        {% endif %}

        current_rows as (
            select
                raw.*
                {% if preserve_created_at and is_incremental() %}
                    , existing.created_at as existing_created_at
                {% endif %}
            from {{ source("airflow_source", raw_table) }} as raw
            {% if is_incremental() %} cross join watermark {% endif %}
            {% if preserve_created_at and is_incremental() %}
                -- carries the row's existing created_at through the merge so it
                -- survives
                -- a later batch touching the same requested_id; see
                -- br_preserved_created_at.
                left join
                    {{ this }} as existing on raw.requested_id = existing.requested_id
            {% endif %}
            where
                -- ids BallotReady returned nothing for land here with a null payload on
                -- purpose, so the landing table (not this transform) can tell "asked,
                -- got
                -- nothing" from "not fetched".
                raw.payload is not null
                {% if is_incremental() %}
                    -- >= not >: a merge on requested_id is idempotent, so
                    -- reprocessing the
                    -- boundary is free, but > would strand a tied loaded_at forever.
                    and raw.loaded_at >= watermark.max_loaded_at
                {% endif %}
            qualify
                row_number() over (
                    partition by raw.requested_id
                    order by raw.loaded_at desc, raw.dag_run_id desc
                )
                = 1
        )
{% endmacro %}
