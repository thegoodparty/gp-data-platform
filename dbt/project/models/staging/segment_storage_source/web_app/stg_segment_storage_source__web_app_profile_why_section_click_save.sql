{% set source_ref = source(
    "segment_storage_source_web_app", "profile_why_section_click_save"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
