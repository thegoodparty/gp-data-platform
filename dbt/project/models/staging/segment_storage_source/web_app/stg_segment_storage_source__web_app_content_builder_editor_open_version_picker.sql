select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "content_builder_editor_open_version_picker",
        )
    }}
