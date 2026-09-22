select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "profile_top_issues_click_finish_entering_issues",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
