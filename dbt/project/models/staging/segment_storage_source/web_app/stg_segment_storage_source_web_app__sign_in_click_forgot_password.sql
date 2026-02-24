{% set source_ref = source(
    "segment_storage_source_web_app", "sign_in_click_forgot_password"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
