-- <=2025 rows must either be archive-rooted (hubspot in source_systems) or
-- TechSpeed-only. A BR, DDHQ, or gp_api row without hubspot means a vendor
-- row reached the mart without merging into the archive: their candidacy /
-- election-stage intermediates stay 2026-gated, so a pre-2026 row keyed to
-- one of them should only ever survive by enriching an archive row or
-- riding a TS-keyed merged row.
select gp_candidacy_stage_id, election_stage_date, source_systems
from {{ ref("candidacy_stage") }}
where
    election_stage_date <= '2025-12-31'
    and not array_contains(source_systems, 'hubspot')
    and (
        array_contains(source_systems, 'ballotready')
        or array_contains(source_systems, 'ddhq')
        or array_contains(source_systems, 'gp_api')
    )
