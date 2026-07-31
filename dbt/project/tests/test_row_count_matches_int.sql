-- m_people_api__voter is one row per LALVOTERID from
-- int__l2_nationwide_uniform_w_haystaq (the left
-- join to turnout scores adds no rows), so the row counts must match.
select *
from
    (
        select
            (select count(*) from {{ ref("m_people_api__voter") }}) as voter_row_count,
            (
                select count(*) from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
            ) as int_row_count
    ) comparison
where voter_row_count != int_row_count
