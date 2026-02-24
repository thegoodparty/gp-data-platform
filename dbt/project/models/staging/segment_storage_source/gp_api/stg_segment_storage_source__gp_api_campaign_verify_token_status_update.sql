select *
from {{ source("segment_storage_source", "campaign_verify_token_status_update") }}
