{% set source_ref = source(
    "segment_storage_source_web_app",
    "candidate_website_started_domain_selection",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
