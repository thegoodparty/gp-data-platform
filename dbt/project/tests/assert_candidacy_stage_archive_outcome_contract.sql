-- <=2025 outcome contract: BR/DDHQ/TS candidacy_stage intermediates are all
-- gated to 2026+, so no row reaching the mart via the merged FOJ path may
-- carry an election_stage_date <= 2025-12-31. FOJ rows never carry 'hubspot'
-- in source_systems and archive rows always do, so the hubspot check is an
-- exact FOJ-path discriminator: it exempts the archive's own legacy
-- DDHQ-match tagging ('ddhq' on pre-2026 archive rows) while still catching
-- a provider row that collides with an archive gp_candidacy_stage_id and
-- displaces it in the mart dedupe.
select gp_candidacy_stage_id, election_stage_date, source_systems
from {{ ref("candidacy_stage") }}
where
    election_stage_date <= '2025-12-31'
    and not array_contains(source_systems, 'hubspot')
    and (
        array_contains(source_systems, 'ballotready')
        or array_contains(source_systems, 'ddhq')
        or array_contains(source_systems, 'techspeed')
    )
