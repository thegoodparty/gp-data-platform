-- Test to ensure that the max updated_at in m_people_api__voter
-- equals the max dbt_valid_from in snapshot__int__l2_nationwide_uniform
select *
from
    (
        select
            (
                select max(updated_at) from {{ ref("m_people_api__voter") }}
            ) as voter_max_updated_at,
            (
                select max(dbt_valid_from)
                from {{ ref("snapshot__int__l2_nationwide_uniform") }}
            ) as snapshot_max_dbt_valid_from
    ) comparison
where voter_max_updated_at != snapshot_max_dbt_valid_from
