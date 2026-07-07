-- <=2025 outcome contract: BR/DDHQ/TS candidacy_stage intermediates are all
-- gated to 2026+, so no row reaching the mart via the merged FOJ path should
-- have an election_stage_date <= 2025-12-31. Archive rows are excluded via
-- anti-join because the archive's own legacy DDHQ-match tagging legitimately
-- carries 'ddhq' in source_systems for pre-2026 rows -- a different,
-- historical matching pipeline, not a leak from the new provider
-- intermediates.
select cs.gp_candidacy_stage_id, cs.election_stage_date, cs.source_systems
from {{ ref("candidacy_stage") }} as cs
left join
    {{ ref("int__civics_candidacy_stage_2025") }} as archive
    on cs.gp_candidacy_stage_id = archive.gp_candidacy_stage_id
where
    archive.gp_candidacy_stage_id is null
    and cs.election_stage_date <= '2025-12-31'
    and (
        array_contains(cs.source_systems, 'ballotready')
        or array_contains(cs.source_systems, 'ddhq')
        or array_contains(cs.source_systems, 'techspeed')
    )
