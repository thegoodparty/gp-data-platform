-- A null voter_count on an L2-backed district is a broken join, not an unknown size,
-- and it silently nulls icp_office_win / icp_office_serve.
with
    l2_districts as (
        select distinct
            state_postal_code as state, district_type, district_name, voter_count
        from {{ ref("int__l2_district_aggregations") }}
    )

select icp.br_database_position_id, icp.l2_district_type, icp.l2_district_name
from {{ ref("int__icp_offices") }} as icp
join
    l2_districts
    on icp.state = l2_districts.state
    and icp.l2_district_type = l2_districts.district_type
    and icp.l2_district_name = l2_districts.district_name
where icp.voter_count is null and l2_districts.voter_count is not null
