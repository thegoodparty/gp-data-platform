-- At most one distinct br_candidate_id per person group. A handful of
-- candidacy-stage clusters carry >1 distinct br_candidate_id (~223 of 460,443
-- BR-containing groups, 0.05% over the all-time historical ER universe; was
-- ~108 before pre-2026 candidacies were included); propagation resolves each
-- such group to its min br_candidate_id and this test flags the residual. Warn
-- on the known set, error on a regression that would indicate an edge bug. No
-- severity override: severity warn would disable error_if entirely.
{{ config(warn_if="!= 0", error_if="> 400") }}

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
