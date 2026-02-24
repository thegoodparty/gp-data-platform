{% set source_ref = source(
    "segment_storage_source_web_app", "polls_poll_results_overview_viewed"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
