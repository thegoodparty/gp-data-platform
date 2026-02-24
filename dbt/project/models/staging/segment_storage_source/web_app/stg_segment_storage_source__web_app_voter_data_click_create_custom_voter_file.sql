select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_data_click_create_custom_voter_file",
        )
    }}
