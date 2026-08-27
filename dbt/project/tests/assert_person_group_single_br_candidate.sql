-- At most one distinct br_candidate_id per person group. Baseline ~585 of
-- ~460K BR-containing groups (0.13%): candidacy-stage clusters carrying >1
-- br_candidate_id (~223), plus contact-edge (E8/E9) merges that are
-- predominantly BR's own cross-cycle duplicates of one person (audited:
-- same name + same state dominates; the residual traces to pre-existing bad
-- native links and DDHQ id reuse, both conflict-flagged). Propagation
-- resolves each group to its min br_candidate_id and this test flags the
-- residual. Warn on the known set, error on a regression that would indicate
-- an edge bug. No severity override: severity warn would disable error_if.
{{ config(warn_if="!= 0", error_if="> 900") }}

with
    br_records as (
        select person_group_key, substring_index(record_key, '|', -1) as br_candidate_id
        from {{ ref("int__civics_person_groups") }}
        where source_name = 'ballotready'
    )

select person_group_key, count(distinct br_candidate_id) as distinct_br
from br_records
group by person_group_key
having count(distinct br_candidate_id) > 1
