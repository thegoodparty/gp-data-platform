-- A request that carries a gateway identity but yields no valid person means
-- the person rule splits one engineer into two: their usage fragments across
-- buckets and every per-person total understates. Error, not warn.
--
-- Selects the request id only. This table carries who made the request, so a
-- failure output must not carry an email or a key name.
--
-- Covers an unparseable key name and a `user_name` that is not a corporate
-- address; the gateway user takes precedence, so a non-corporate login would
-- win over a valid key name.
select request_id
from {{ ref("stg_airflow_source__bifrost_request_log") }}
where
    -- Blank is not identity: staging nullifs both columns before the rule, so
    -- this has to test them the same way.
    (
        nullif(trim(user_name), '') is not null
        or nullif(trim(virtual_key_name), '') is not null
    )
    and (
        person_email is null
        or not person_email rlike '^[a-z0-9._%+-]+@goodparty[.]org$'
    )
