/*
This model creates the district table in the mart layer using the election_api district table.
It is used to serve the district information to the people-api schema.
See https://github.com/thegoodparty/people-api/blob/develop/prisma/schema/District.prisma#L55-L60
*/
{{
    config(
        materialized="view",
        tags=["mart", "people_api", "district"],
    )
}}

select
    id,
    created_at,
    updated_at,
    l2_district_type as type,
    l2_district_name as name,
    state
from {{ ref("m_election_api__district") }}
