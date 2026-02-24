{% set source_ref = source(
    "segment_storage_source_web_app", "settings_notifications_toggle_email"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
