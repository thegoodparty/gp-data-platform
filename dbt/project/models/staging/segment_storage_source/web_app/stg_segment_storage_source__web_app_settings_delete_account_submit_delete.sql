select *
from
    {{
        source(
            "segment_storage_source_web_app", "settings_delete_account_submit_delete"
        )
    }}
