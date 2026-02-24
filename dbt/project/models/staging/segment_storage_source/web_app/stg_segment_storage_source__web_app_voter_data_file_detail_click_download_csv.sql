select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "voter_data_file_detail_click_download_csv",
        )
    }}
