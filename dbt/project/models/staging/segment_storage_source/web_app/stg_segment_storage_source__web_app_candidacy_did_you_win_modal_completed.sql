select *
from
    {{
        source(
            "segment_storage_source_web_app", "candidacy_did_you_win_modal_completed"
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
