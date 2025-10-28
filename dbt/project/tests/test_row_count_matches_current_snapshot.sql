-- Test to ensure that the row count in m_people_api__voter
-- equals the row count in snapshot__int__l2_nationwide_uniform where dbt_valid_to is
-- null
select *
from
    (
        select
            (select count(*) from {{ ref("m_people_api__voter") }}) as voter_row_count,
            (
                select count(*)
                from {{ ref("snapshot__int__l2_nationwide_uniform") }}
                where dbt_valid_to is null
            ) as snapshot_current_row_count
    ) comparison
where voter_row_count != snapshot_current_row_count
