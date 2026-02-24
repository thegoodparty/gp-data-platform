{% set source_ref = source(
    "segment_storage_source_web_app",
    "pro_upgrade_committee_check_page_hover_name_of_campaign_committee_help",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
