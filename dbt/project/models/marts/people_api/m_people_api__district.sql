/*
This model creates the district table in the mart layer using the election_api district table.
It is used to serve the district information to the people-api schema.
See https://github.com/thegoodparty/people-api/blob/develop/prisma/schema/District.prisma#L55-L60
*/
{{
    config(
        materialized="view",
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
-- Drop the single country-scope row (state='US', a Country district). The serving
-- contract types
-- District.state as the USState enum (50 states + DC), which has no 'US' value; prod
-- never carried
-- this row, and nothing references it (no DistrictVoter link, no DistrictStats row).
-- Excluding it
-- keeps District.state within the enum without extending it.
where state <> 'US'
