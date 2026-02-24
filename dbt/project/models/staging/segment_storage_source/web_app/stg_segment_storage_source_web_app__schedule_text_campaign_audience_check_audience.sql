{% set source_ref = source(
    "segment_storage_source_web_app",
    "schedule_text_campaign_audience_check_audience",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
