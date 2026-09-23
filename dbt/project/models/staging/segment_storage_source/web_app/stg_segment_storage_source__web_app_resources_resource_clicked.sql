select *
from {{ source("segment_storage_source_web_app", "resources_resource_clicked") }}
