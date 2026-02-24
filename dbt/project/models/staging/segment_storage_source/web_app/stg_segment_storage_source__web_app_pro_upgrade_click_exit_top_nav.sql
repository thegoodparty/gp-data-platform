{% set source_ref = source(
    "segment_storage_source_web_app", "pro_upgrade_click_exit_top_nav"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
