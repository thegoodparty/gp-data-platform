-- m_people_api__voter derives updated_at directly from
-- int__l2_nationwide_uniform_w_haystaq's
-- loaded_at, so a mart-vs-int check would be tautological. Instead verify the
-- (incrementally
-- merged) w_haystaq intermediate actually picked up the latest L2 batch, by comparing
-- its max
-- loaded_at against the upstream int__l2_nationwide_uniform. Null-aware, and the
-- is-null guard
-- also fails an all-null loaded_at (both maxes null are "not distinct").
select *
from
    (
        select
            (
                select max(loaded_at) from {{ ref("int__l2_nationwide_uniform") }}
            ) as source_max_loaded_at,
            (
                select max(loaded_at)
                from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
            ) as int_max_loaded_at
    ) comparison
where
    source_max_loaded_at is distinct from int_max_loaded_at
    or source_max_loaded_at is null
