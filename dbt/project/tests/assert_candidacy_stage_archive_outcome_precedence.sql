-- <=2025 outcome precedence (supersedes the interim absence test from the
-- archive-outcome-guard PR): where a person-stage has a HubSpot archive row
-- carrying a non-null outcome field, the merged mart row must display the
-- archive's value. The archive-era merge keeps the archive row's
-- gp_candidacy_stage_id and coalesces display fields archive-first, so each
-- field must match wherever the archive supplied one. A failure means an
-- enriching FOJ row displaced an archive value.
with
    archive as (
        select
            gp_candidacy_stage_id,
            is_winner as archive_is_winner,
            election_result as archive_result,
            election_result_source as archive_result_source,
            votes_received as archive_votes
        from {{ ref("int__civics_candidacy_stage_2025") }}
    )

select
    cs.gp_candidacy_stage_id,
    cs.is_winner,
    a.archive_is_winner,
    cs.election_result,
    a.archive_result,
    cs.election_result_source,
    a.archive_result_source,
    cs.votes_received,
    a.archive_votes
from {{ ref("candidacy_stage") }} as cs
inner join archive as a using (gp_candidacy_stage_id)
where
    (a.archive_is_winner is not null and not cs.is_winner <=> a.archive_is_winner)
    or (a.archive_result is not null and not cs.election_result <=> a.archive_result)
    or (
        a.archive_result_source is not null
        and not cs.election_result_source <=> a.archive_result_source
    )
    or (a.archive_votes is not null and not cs.votes_received <=> a.archive_votes)
