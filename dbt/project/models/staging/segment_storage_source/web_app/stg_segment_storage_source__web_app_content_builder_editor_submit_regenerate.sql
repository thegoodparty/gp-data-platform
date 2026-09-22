select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "content_builder_editor_submit_regenerate",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
