{% set source_ref = source(
    "segment_storage_source", "account_password_reset_requested"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
