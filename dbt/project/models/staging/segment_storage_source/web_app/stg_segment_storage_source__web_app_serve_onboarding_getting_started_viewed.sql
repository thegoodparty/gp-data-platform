select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "serve_onboarding_getting_started_viewed",
        )
    }}
