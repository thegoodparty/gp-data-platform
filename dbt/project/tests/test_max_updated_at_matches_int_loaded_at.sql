-- m_people_api__voter reads int__l2_nationwide_uniform_w_haystaq directly, so its max
-- updated_at
-- (sourced from the L2 loaded_at) must equal the source's max loaded_at.
select *
from
    (
        select
            (
                select max(updated_at) from {{ ref("m_people_api__voter") }}
            ) as voter_max_updated_at,
            (
                select max(loaded_at)
                from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
            ) as int_max_loaded_at
    ) comparison
where voter_max_updated_at != int_max_loaded_at
