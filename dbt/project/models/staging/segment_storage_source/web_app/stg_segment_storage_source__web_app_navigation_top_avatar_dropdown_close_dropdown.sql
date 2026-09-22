select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "navigation_top_avatar_dropdown_close_dropdown",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
