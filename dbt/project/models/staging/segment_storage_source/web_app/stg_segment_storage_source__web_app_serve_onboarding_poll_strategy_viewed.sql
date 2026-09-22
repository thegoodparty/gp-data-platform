select *
from
    {{
        source(
            "segment_storage_source_web_app", "serve_onboarding_poll_strategy_viewed"
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
