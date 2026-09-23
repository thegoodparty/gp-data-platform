select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "profile_top_issues_click_finish_entering_issues",
        )
    }}
