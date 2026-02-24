{% set source_ref = source(
    "segment_storage_source_web_app", "candidate_website_continued"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
