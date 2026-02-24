select * from {{ source("segment_storage_source", "account_password_reset_requested") }}
