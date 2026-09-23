select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "schedule_text_campaign_audience_enter_audience_request",
        )
    }}
