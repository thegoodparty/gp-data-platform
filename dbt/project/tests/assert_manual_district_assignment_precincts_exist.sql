-- A precinct that does not exist in the voter file assigns nobody and the build
-- stays green, so a transposed number or a format mismatch is invisible -- L2
-- zero-pads precincts, so '7' never matches '07'. Matched on state and county
-- only: a precinct split across the city line can legitimately contribute no
-- in-city voters, so requiring the full tuple to match would fail on good data.
with
    voter_precincts as (
        select distinct state_postal_code, county, precinct
        from {{ ref("int__l2_nationwide_uniform_raw_districts") }}
        where
            state_postal_code
            in (select state from {{ ref("l2_manual_district_assignments") }})
    )
select assignments.state, assignments.county, assignments.precinct
from {{ ref("l2_manual_district_assignments") }} as assignments
left join
    voter_precincts
    on voter_precincts.state_postal_code = assignments.state
    and voter_precincts.precinct = assignments.precinct
    and (assignments.county is null or assignments.county = voter_precincts.county)
where assignments.precinct is not null and voter_precincts.precinct is null
