select * from {{ source("segment_storage_source_web_app", "sign_up_click_login") }}
