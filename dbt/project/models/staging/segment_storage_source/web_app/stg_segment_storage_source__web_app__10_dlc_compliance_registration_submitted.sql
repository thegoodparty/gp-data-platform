select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "_10_dlc_compliance_registration_submitted",
        )
    }}
