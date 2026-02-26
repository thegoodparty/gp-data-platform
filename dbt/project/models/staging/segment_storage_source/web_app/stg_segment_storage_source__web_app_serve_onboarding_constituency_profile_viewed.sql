select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "serve_onboarding_constituency_profile_viewed",
        )
    }}
