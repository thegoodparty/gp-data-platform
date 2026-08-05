-- L2 omits municipal district geography for some cities, so those seats have no
-- district to resolve against. The seed fills the gap from published precinct
-- maps; coalesce keeps L2 authoritative wherever it does supply a value.
-- Backticks preserve the source column's casing, which the people-api export
-- carries through to its Postgres column names.
-- Databricks freezes the `*` expansion when the view is created, so a run that
-- rebuilds the raw model alone must also rebuild this or new L2 columns stop
-- reaching downstream.
select
    voters.* except (`City_Council_Commissioner_District`),
    coalesce(
        voters.`City_Council_Commissioner_District`, assignments.l2_district_name
    ) as `City_Council_Commissioner_District`
from {{ ref("int__l2_nationwide_uniform_raw_districts") }} as voters
left join
    {{ ref("l2_manual_district_assignments") }} as assignments
    on assignments.l2_district_type = 'City_Council_Commissioner_District'
    and assignments.state = voters.state_postal_code
    and (assignments.county is null or assignments.county = voters.county)
    and (assignments.city is null or assignments.city = voters.city)
    and (assignments.precinct is null or assignments.precinct = voters.precinct)
