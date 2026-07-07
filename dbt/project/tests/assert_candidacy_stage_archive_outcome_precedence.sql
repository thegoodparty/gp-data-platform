-- <=2025 outcome precedence (decision 4, supersedes the PR2 absence test):
-- where a person-stage has a HubSpot archive row carrying a non-null outcome,
-- the merged mart row must display that archive outcome. The archive-era merge
-- keeps the archive row's gp_candidacy_stage_id and coalesces display fields
-- archive-first, so the mart's election_result must equal the archive's
-- wherever the archive supplied one. A failure means an enriching FOJ row
-- displaced the archive outcome.
with
    archive as (
        select gp_candidacy_stage_id, election_result as archive_result
        from {{ ref("int__civics_candidacy_stage_2025") }}
        where election_result is not null
    )

select cs.gp_candidacy_stage_id, cs.election_result, a.archive_result
from {{ ref("candidacy_stage") }} as cs
inner join archive as a using (gp_candidacy_stage_id)
where not (cs.election_result <=> a.archive_result)
