select * from {{ source("segment_storage_source", "poll_results_synthesis_complete") }}
