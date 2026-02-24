select *
from {{ source("segment_storage_source_web_app", "profile_office_details_click_edit") }}
