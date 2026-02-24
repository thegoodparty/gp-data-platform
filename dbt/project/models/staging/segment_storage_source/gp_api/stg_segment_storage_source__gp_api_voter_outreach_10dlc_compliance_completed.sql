select *
from {{ source("segment_storage_source", "voter_outreach_10dlc_compliance_completed") }}
