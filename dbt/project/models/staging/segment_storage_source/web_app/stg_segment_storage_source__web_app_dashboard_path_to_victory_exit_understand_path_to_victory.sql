select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "dashboard_path_to_victory_exit_understand_path_to_victory",
        )
    }}
