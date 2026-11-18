-- A request that carries a gateway identity but yields no valid person means the person rule is
-- splitting one engineer into two: their usage would fragment across buckets, and every
-- per-person total in the mart would understate. Error, not warn, for that reason.
--
-- Selects the request id only: this table carries who made the request, so a failure output must
-- not carry an email or a key name.
--
-- Covers an unparseable key name and a `user_name` that is not an `@goodparty.org` address (the
-- gateway user takes precedence, so a non-corporate login would win over a valid key name).
select request_id
from {{ ref("stg_airflow_source__bifrost_request_log") }}
where
    (virtual_key_name is not null or user_name is not null)
    and (
        person_email is null
        or not person_email rlike '^[a-z0-9._%+-]+@goodparty[.]org$'
    )
