select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "pro_upgrade_committee_check_page_toggle_ein_requirement",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
