{% macro ddhq_gdrive_election_results_merged() %}
    -- One-off master backfill under the live incremental feed. Master is
    -- authoritative within its horizon; the incremental table adds only later
    -- dates. A plain union would re-introduce the canceled rows master dropped.
    -- Identical, same-ordered columns, so the positional union is safe.
    select m.*
    from {{ source("airbyte_source", "ddhq_gdrive_election_results_master") }} as m

    union all

    select i.*
    from {{ source("airbyte_source", "ddhq_gdrive_election_results") }} as i
    where
        cast(i.date as date) > (
            -- coalesce: an empty master makes max() NULL, dropping every row.
            select coalesce(max(cast(date as date)), cast('1900-01-01' as date))
            from {{ source("airbyte_source", "ddhq_gdrive_election_results_master") }}
        )
{% endmacro %}
