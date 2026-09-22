select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "navigation_dashboard_click_voter_outreach",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
