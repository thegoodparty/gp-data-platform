{% set source_ref = source(
    "segment_storage_source_web_app", "profile_office_details_click_edit"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
