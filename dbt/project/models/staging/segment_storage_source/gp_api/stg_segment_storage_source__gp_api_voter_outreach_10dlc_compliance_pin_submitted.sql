select *
from
    {{
        source(
            "segment_storage_source",
            "voter_outreach_10dlc_compliance_pin_submitted",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
