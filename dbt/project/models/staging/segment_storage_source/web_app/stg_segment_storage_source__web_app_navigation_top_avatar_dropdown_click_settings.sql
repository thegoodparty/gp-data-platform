select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "navigation_top_avatar_dropdown_click_settings",
        )
    }}
