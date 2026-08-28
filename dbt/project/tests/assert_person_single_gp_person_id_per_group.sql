-- Every record in a person group must resolve to the same gp_person_id. The
-- mint joins one minting_member per group, so this is a guard against a
-- grouping or ranking regression.
select person_group_key, count(distinct gp_person_id) as distinct_ids
from {{ ref("int__civics_person_canonical_ids") }}
group by person_group_key
having count(distinct gp_person_id) > 1
