select * from {{ source("segment_storage_source", "onboarding_user_created") }}
