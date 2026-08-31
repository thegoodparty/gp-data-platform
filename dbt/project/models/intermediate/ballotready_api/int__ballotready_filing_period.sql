select created_at, database_id, end_on, id, notes, start_on, type, updated_at
from {{ ref("stg_airflow_source__ballotready_filing_period_raw") }}
