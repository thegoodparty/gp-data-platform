select *
from
    {{
        source(
            "segment_storage_source_web_app", "profile_running_against_submit_add_new"
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
