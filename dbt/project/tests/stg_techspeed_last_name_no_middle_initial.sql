-- A1: staging last_name should not match the bug-pattern regex after
-- the Phase 2 parser is applied. Covers leading single-initial, leading
-- multi-initial (catches "A. C. Campbell"-style cases), and trailing
-- single-initial. Compound surnames are excluded by the parser's
-- lookarounds and shouldn't match either.
select last_name
from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }}
where
    last_name is not null
    and (
        last_name rlike '^[A-Z][.] ?[A-Za-z]'
        or last_name rlike '^[A-Z] [A-Za-z]'
        or last_name rlike '[A-Za-z] [A-Z]$'
    )
