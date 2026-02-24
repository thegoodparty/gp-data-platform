select *
from
    {{
        source(
            "segment_storage_source_web_app", "settings_personal_info_click_upload"
        )
    }}
