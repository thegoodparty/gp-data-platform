select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "pro_upgrade_committee_check_page_hover_ein_number_help",
        )
    }}
