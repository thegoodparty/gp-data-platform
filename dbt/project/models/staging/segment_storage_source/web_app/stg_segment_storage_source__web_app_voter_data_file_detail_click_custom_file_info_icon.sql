select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_data_file_detail_click_custom_file_info_icon",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
