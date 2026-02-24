select *
from
    {{
        source(
            "segment_storage_source_web_app", "navigation_top_click_avatar_dropdown"
        )
    }}
