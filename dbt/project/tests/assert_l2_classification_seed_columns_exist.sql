-- Every column classified in the l2_column_classification seed must exist in
-- int__l2_nationwide_uniform_w_haystaq. serve_agent_voters and win_agent_voters
-- generate their projections from this seed via run_query, so a seed entry with
-- no matching column produces SQL naming a column that isn't there and the view
-- fails to build (or to read) with UNRESOLVED_COLUMN.
--
-- This is invisible to l2_uniform_schema_preflight, which only diffs target
-- columns against the upstream contract and never the reverse, and never looks
-- at the seed. It is also invisible for as long as nothing recreates the views:
-- the merge strategy with append_new_columns keeps a retired vendor column
-- forever, serving frozen values, so the seed and the table drift apart
-- silently until a full refresh finally drops it.
{% set w_haystaq = ref("int__l2_nationwide_uniform_w_haystaq") %}

with
    target_columns as (
        select lower(column_name) as column_name
        from {{ w_haystaq.database }}.information_schema.columns
        where
            lower(table_schema) = lower('{{ w_haystaq.schema }}')
            and lower(table_name) = lower('{{ w_haystaq.identifier }}')
            and column_name is not null
    ),
    classified as (
        select column_name, family, is_available
        from {{ ref("l2_column_classification") }}
    )
select classified.column_name, classified.family, classified.is_available
from classified
left join target_columns on target_columns.column_name = lower(classified.column_name)
where target_columns.column_name is null
