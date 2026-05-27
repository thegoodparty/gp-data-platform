-- A2: end-to-end assertion on mart_civics.candidate for TS-source-systems
-- rows. Filtered to exclude rows that ALSO carry the 'hubspot' source
-- (legacy 2025 archive, addressed by a separate ticket).
select gp_candidate_id, last_name, source_systems
from {{ ref("candidate") }}
where
    last_name is not null
    and array_contains(source_systems, 'techspeed')
    and not array_contains(source_systems, 'hubspot')
    and (
        last_name rlike '^[A-Z][.] ?[A-Za-z]'
        or last_name rlike '^[A-Z] [A-Za-z]'
        or last_name rlike '[A-Za-z] [A-Z]$'
    )
