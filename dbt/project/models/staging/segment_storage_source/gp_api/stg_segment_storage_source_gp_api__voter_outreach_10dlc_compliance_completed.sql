{% set source_ref = source(
    "segment_storage_source", "voter_outreach_10dlc_compliance_completed"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
