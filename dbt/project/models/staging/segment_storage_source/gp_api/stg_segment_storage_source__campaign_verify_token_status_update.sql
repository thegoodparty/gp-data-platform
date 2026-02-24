{% set source_ref = source(
    "segment_storage_source", "campaign_verify_token_status_update"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
