{% set source_ref = source(
    "segment_storage_source_web_app",
    "voter_outreach_10dlc_compliance_started",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
