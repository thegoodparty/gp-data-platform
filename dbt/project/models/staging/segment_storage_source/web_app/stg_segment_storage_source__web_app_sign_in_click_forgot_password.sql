select *
from {{ source("segment_storage_source_web_app", "sign_in_click_forgot_password") }}
