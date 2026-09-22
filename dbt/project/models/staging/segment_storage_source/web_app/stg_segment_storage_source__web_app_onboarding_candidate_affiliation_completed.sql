select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "onboarding_candidate_affiliation_completed",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
