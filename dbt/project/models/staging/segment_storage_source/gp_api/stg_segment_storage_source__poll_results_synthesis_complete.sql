{% set source_ref = source(
    "segment_storage_source", "poll_results_synthesis_complete"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
