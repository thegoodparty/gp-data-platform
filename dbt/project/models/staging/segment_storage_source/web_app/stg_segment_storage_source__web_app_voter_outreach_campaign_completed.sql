select *
from {{ source("segment_storage_source_web_app", "voter_outreach_campaign_completed") }}
