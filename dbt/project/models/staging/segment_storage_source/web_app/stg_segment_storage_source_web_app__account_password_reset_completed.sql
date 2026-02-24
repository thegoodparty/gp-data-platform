{% set source_ref = source(
    "segment_storage_source_web_app", "account_password_reset_completed"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
