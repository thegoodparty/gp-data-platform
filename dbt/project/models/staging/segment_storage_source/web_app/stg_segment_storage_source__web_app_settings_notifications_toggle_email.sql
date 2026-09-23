select *
from
    {{
        source(
            "segment_storage_source_web_app", "settings_notifications_toggle_email"
        )
    }}
