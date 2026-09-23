select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "onboarding_complete_step_click_go_to_dashboard",
        )
    }}
