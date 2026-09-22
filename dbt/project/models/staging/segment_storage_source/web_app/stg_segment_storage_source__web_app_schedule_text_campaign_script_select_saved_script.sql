select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "schedule_text_campaign_script_select_saved_script",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
