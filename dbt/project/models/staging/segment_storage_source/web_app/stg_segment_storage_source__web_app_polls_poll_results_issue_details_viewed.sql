select *
from
    {{
        source(
            "segment_storage_source_web_app",
            "polls_poll_results_issue_details_viewed",
        )
    }}
