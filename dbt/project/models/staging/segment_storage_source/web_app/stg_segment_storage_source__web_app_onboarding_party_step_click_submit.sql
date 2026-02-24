select *
from
    {{ source("segment_storage_source_web_app", "onboarding_party_step_click_submit") }}
