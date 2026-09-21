-- Exactly one account per person group carries is_primary_account, so a
-- consumer can take one row per person with a simple filter.
--
-- Two-sided on purpose. Zero primaries is the likelier regression than two:
-- the person graph deliberately holds gp-api members that this model has no
-- row for, so a rank computed over the graph rather than over the spine gives
-- a person no primary at all, and a duplicates-only check passes straight
-- through it.
select gp_person_id, sum(case when is_primary_account then 1 else 0 end) as primaries
from {{ ref("int__user_resolved_keys") }}
group by gp_person_id
having sum(case when is_primary_account then 1 else 0 end) <> 1
