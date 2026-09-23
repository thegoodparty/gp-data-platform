select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "settings_account_settings_click_upgrade",
        )
    }}
