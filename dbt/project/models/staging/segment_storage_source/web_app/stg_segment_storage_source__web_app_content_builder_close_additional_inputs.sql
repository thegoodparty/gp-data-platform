select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "content_builder_close_additional_inputs",
        )
    }}
