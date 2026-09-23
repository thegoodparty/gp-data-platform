select *
from
    {{
        source(
            "segment_storage_source_web_app", "serve_onboarding_success_page_viewed"
        )
    }}
