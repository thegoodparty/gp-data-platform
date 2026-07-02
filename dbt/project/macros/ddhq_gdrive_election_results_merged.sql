{% macro ddhq_gdrive_election_results_merged() %}
    -- Merge the one-off consolidated master backfill (delivered 2026-07-01) with
    -- the live incremental DDHQ results feed.
    --
    -- The master file re-states all results through DDHQ's data horizon: it adds
    -- the previously-missing October 2025 races and removes canceled/unpublished
    -- races that the incremental table still carries. Because those canceled rows
    -- only exist in the incremental table, a plain UNION would re-introduce them.
    --
    -- So master is authoritative for everything within its coverage window, and
    -- the incremental table contributes only rows for election dates BEYOND the
    -- master horizon (i.e. go-forward deliveries). Canceled races are past events
    -- (<= horizon), so they stay excluded; genuinely new elections flow in from
    -- the incremental feed automatically with no further changes.
    --
    -- Both raw tables share an identical, same-ordered column layout, so the
    -- positional UNION ALL below is safe. Used by the election_results and
    -- election_results_invalid staging models so they operate on one row set.
    select m.*
    from {{ source("airbyte_source", "ddhq_gdrive_election_results_master") }} as m

    union all

    select i.*
    from {{ source("airbyte_source", "ddhq_gdrive_election_results") }} as i
    where
        cast(i.date as date) > (
            -- coalesce guards the empty-master case: an empty master makes
            -- max() NULL, and `date > NULL` is UNKNOWN, which would silently
            -- drop every incremental row. Flooring to 1900-01-01 instead lets
            -- the full incremental feed through (master contributes nothing).
            select coalesce(max(cast(date as date)), cast('1900-01-01' as date))
            from {{ source("airbyte_source", "ddhq_gdrive_election_results_master") }}
        )
{% endmacro %}
