select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "serve_onboarding_meet_your_constituents_viewed",
        )
    }}
