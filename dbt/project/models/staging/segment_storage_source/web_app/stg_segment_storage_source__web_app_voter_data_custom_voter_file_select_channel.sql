select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_data_custom_voter_file_select_channel",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
