{% set source_ref = source(
    "segment_storage_source_web_app", "settings_delete_account_click_delete"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
