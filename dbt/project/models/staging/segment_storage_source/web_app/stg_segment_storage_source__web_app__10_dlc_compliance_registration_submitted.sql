select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "_10_dlc_compliance_registration_submitted",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
