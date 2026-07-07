-- <=2025 outcome contract: the BR and DDHQ candidacy_stage intermediates
-- carry their own 2026 gates, keeping the HubSpot archive the <=2025 outcome
-- authority for those providers. This test pins that contract before
-- all-time ER lands. TechSpeed is intentionally allowed: <=2025 TS-only rows
-- are real coverage the archive lacks; per-person HubSpot deference ships
-- with the person-layer FKs. FOJ rows never carry 'hubspot' in
-- source_systems and archive rows always do, so the hubspot check is an
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
    )
