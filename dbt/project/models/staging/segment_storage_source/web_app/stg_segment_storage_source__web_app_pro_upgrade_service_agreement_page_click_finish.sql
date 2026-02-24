{% set source_ref = source(
    "segment_storage_source_web_app",
    "pro_upgrade_service_agreement_page_click_finish",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
