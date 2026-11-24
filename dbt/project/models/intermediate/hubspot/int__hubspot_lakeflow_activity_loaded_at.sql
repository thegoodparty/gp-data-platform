-- The Lakeflow loader time per activity. Lakeflow has no loader timestamp
-- column, only `_metadata.file_modification_time`.
--
-- A table, not the intermediate default view: `_metadata` is lost when the scan
-- sits behind something inlined twice, and a staging view that projected it
-- could not be read twice, which the touch history does and the build failed on.
{{ config(materialized="table") }}

select
    'CALL' as activity_type,
    hs_object_id as activity_id,
    _metadata.file_modification_time as loaded_at
from {{ source("lakeflow_source", "calls") }}

union all

select 'EMAIL', hs_object_id, _metadata.file_modification_time
from {{ source("lakeflow_source", "emails") }}

union all

select 'MEETING', hs_object_id, _metadata.file_modification_time
from {{ source("lakeflow_source", "meetings") }}

union all

select 'NOTE', hs_object_id, _metadata.file_modification_time
from {{ source("lakeflow_source", "notes") }}

union all

select 'TASK', hs_object_id, _metadata.file_modification_time
from {{ source("lakeflow_source", "tasks") }}
