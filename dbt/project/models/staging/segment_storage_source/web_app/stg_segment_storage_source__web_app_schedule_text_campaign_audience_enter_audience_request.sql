select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "schedule_text_campaign_audience_enter_audience_request",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
