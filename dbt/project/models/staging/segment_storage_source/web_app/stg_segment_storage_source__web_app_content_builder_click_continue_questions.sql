select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "content_builder_click_continue_questions",
        )
    }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
