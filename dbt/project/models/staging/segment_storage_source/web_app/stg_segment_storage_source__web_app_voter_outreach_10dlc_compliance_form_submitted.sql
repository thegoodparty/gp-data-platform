select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_outreach_10dlc_compliance_form_submitted",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
