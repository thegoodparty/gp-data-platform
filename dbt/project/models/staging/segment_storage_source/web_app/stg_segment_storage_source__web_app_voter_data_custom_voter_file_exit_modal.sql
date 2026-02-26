select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_data_custom_voter_file_exit_modal",
        )
    }}
