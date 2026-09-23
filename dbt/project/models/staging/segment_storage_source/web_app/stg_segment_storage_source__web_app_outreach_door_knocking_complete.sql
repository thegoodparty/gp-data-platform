select *
from {{ source("segment_storage_source_web_app", "outreach_door_knocking_complete") }}
