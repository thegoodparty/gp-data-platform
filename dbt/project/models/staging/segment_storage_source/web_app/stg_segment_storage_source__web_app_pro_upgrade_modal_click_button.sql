select *
from {{ source("segment_storage_source_web_app", "pro_upgrade_modal_click_button") }}
