select *
from
    {{
        source(
            "segment_storage_source_web_app", "onboarding_candidate_office_completed"
        )
    }}
