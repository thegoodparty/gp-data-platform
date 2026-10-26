-- Keyed by the same salted id as gp_api_voters, so gp-api filters with `id in (...)`
-- without touching the voter table. Untagged so it refreshes with the daily uploads;
-- a table, not the folder's default view, so the id hash isn't recomputed per request.
{{ config(materialized="table") }}

select
    {{ generate_salted_uuid(fields=["LALVOTERID"], salt="l2") }} as id,
    `LALVOTERID`,
    true as is_eav_general_2026
from {{ source("l2_eav", "l2_eav_general_2026") }}
