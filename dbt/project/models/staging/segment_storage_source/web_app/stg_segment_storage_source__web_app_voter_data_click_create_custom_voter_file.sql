select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_data_click_create_custom_voter_file",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
