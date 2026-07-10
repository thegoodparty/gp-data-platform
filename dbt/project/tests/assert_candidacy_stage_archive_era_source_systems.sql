-- <=2025 rows without a hubspot root must be TechSpeed-anchored: the BR/DDHQ
-- candidacy and election-stage intermediates stay 2026-gated, so only a TS trio
-- carries FK-valid pre-2026 ids. A BR/DDHQ/gp_api row that is neither
-- hubspot-rooted nor part of a TS-keyed cluster reached the mart without either
-- enriching an archive row or riding a TS-keyed merged row. Looser election-date
-- matching now clusters BR/DDHQ into TS-anchored clusters, so source_systems
-- credits them alongside techspeed -- expected, not a leak.
select gp_candidacy_stage_id, election_stage_date, source_systems
from {{ ref("candidacy_stage") }}
where
    election_stage_date <= '2025-12-31'
    and not array_contains(source_systems, 'hubspot')
    and not array_contains(source_systems, 'techspeed')
    and (
        array_contains(source_systems, 'ballotready')
        or array_contains(source_systems, 'ddhq')
        or array_contains(source_systems, 'gp_api')
    )
