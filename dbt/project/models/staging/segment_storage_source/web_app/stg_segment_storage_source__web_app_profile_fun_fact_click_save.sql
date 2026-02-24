select *
from {{ source("segment_storage_source_web_app", "profile_fun_fact_click_save") }}
