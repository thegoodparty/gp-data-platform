select *
from {{ source("segment_storage_source", "content_builder_generation_completed") }}
