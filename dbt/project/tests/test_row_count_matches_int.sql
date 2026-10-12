-- gp_api_voters is one row per LALVOTERID from
-- int__l2_nationwide_uniform_w_haystaq (the left
-- join to turnout scores adds no rows), so the row counts must match.
select *
from
    (
        select
            (select count(*) from {{ ref("gp_api_voters") }}) as voter_row_count,
            (
                select count(*) from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
            ) as int_row_count
    ) comparison
-- null-aware: a plain != passes silently if a count subquery yields null
-- (inaccessible model).
where voter_row_count is distinct from int_row_count
