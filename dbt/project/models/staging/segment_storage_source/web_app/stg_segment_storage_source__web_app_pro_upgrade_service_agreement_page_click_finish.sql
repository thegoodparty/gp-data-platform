select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "pro_upgrade_service_agreement_page_click_finish",
        )
    }}
