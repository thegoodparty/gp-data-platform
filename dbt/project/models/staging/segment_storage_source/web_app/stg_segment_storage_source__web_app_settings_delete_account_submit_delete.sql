select *
from
    {{
        source(
            "segment_storage_source_web_app", "settings_delete_account_submit_delete"
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
