select *
from {{ source("segment_storage_source_web_app", "custom_voter_file_created") }}
