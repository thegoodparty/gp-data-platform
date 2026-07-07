-- At most one distinct br_candidate_id per person group. A handful of
-- candidacy-stage clusters carry >1 distinct br_candidate_id (~108 of 70,960,
-- 0.15% per plan finding 9); propagation resolves each such group to its min
-- br_candidate_id and this test flags the residual. Warn on the known set,
-- error on a regression that would indicate an edge bug.
{{ config(severity="warn", error_if="> 200") }}

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
