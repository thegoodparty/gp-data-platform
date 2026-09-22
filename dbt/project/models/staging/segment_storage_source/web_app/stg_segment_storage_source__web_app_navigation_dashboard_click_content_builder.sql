select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "navigation_dashboard_click_content_builder",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
