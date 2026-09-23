select *
from
    {{
        source(
            "segment_storage_source_web_app", "profile_campaign_details_click_save"
        )
    }}
