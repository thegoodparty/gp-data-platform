select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "settings_account_settings_click_upgrade",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
