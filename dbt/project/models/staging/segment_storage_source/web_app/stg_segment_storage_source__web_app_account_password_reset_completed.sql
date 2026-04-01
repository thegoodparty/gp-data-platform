select *
from {{ source("segment_storage_source_web_app", "account_password_reset_completed") }}
