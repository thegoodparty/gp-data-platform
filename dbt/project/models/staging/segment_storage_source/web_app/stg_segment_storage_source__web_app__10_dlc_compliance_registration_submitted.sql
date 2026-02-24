{% set source_ref = source(
    "segment_storage_source_web_app",
    "_10_dlc_compliance_registration_submitted",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
