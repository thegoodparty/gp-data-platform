select
    database_id,
    id,
    key,
    name,
    plugin_enabled,
    response_type,
    row_order,
    created_at,
    updated_at
from {{ ref("stg_airflow_source__ballotready_issue_raw") }}
