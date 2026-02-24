select * from {{ source("segment_storage_source", "identifies") }}
