select *
from
    {{ source("segment_storage_source_web_app", "profile_running_against_click_save") }}
