{% set source_ref = source(
    "segment_storage_source_web_app",
    "settings_account_settings_click_upgrade",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
