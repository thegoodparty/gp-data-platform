select *
from {{ source("segment_storage_source_web_app", "profile_why_section_click_save") }}
