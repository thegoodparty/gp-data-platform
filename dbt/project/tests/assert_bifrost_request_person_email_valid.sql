-- A row carrying identity but yielding no valid person would split one
-- engineer into two buckets and understate every total, so this errors.
-- Selects the id only: a failure output must carry no email or key name.
select request_id
from {{ ref("stg_airflow_source__bifrost_request_log") }}
where
    -- Blank is not identity; staging nullifs both columns first.
    (
        nullif(trim(user_name), '') is not null
        or nullif(trim(virtual_key_name), '') is not null
    )
    and (
        person_email is null
        or not person_email rlike '^[a-z0-9._%+-]+@goodparty[.]org$'
    )
