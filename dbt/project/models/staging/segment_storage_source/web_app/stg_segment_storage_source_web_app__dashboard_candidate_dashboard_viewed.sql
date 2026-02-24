{% set source_ref = source(
    "segment_storage_source_web_app", "dashboard_candidate_dashboard_viewed"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
