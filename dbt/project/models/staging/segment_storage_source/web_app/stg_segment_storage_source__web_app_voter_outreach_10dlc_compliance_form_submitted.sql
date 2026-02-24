select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_outreach_10dlc_compliance_form_submitted",
        )
    }}
