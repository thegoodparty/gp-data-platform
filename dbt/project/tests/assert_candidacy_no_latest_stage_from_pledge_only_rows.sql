-- gp_api-only candidacy_stage rows record the campaign's pledged (intended)
-- race, not ballot evidence, and are excluded from stage derivation in the
-- candidacy mart. A candidacy whose stage rows are ALL gp_api-only therefore
-- has no evidence row at all (in-context or not), so its latest_stage_reached
-- must be NULL -- and with no in-context stage row and no archive fallback,
-- its candidacy_result must be NULL too (guards the pledged-runoff rows that
-- once read 'Runoff'). Archive-era candidacies cannot enter the population
-- (archive stage rows always carry 'hubspot'), and a NULL source_systems row
-- counts as evidence (fail open), matching the mart predicate. Zero rows
-- expected.
with
    pledge_only_candidacies as (
        select gp_candidacy_id
        from {{ ref("candidacy_stage") }}
        group by gp_candidacy_id
        having count(*) = count_if(source_systems <=> array('gp_api'))
    )

select c.gp_candidacy_id, c.latest_stage_reached, c.candidacy_result, c.source_systems
from {{ ref("candidacy") }} as c
inner join pledge_only_candidacies as p on c.gp_candidacy_id = p.gp_candidacy_id
where c.latest_stage_reached is not null or c.candidacy_result is not null
