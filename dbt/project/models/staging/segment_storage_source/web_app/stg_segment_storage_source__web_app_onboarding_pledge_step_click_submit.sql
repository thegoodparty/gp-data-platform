select *
from
    {{
        source(
            "segment_storage_source_web_app", "onboarding_pledge_step_click_submit"
        )
    }}
