{% set source_ref = source(
    "segment_storage_source_web_app", "settings_personal_info_click_upload"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
