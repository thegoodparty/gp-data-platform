select *
from {{ source("segment_storage_source_web_app", "pro_upgrade_click_go_to_stripe") }}
