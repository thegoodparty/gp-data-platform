-- A county or city key that matches no voter assigns nobody and the build stays
-- green, so a misspelled county or a city value that drifts from L2's spelling
-- ('GOODLETTSVILLE CITY' vs 'GOODLETTSVILLE CITY (EST.)') is invisible: the
-- position then resolves to whatever the district already held. The precinct
-- test covers rows keyed on a precinct; this covers the rest of the sparse
-- tuple, matched the way the uniform view matches it.
with
    voter_places as (
        select distinct state_postal_code, county, city
        from {{ ref("int__l2_nationwide_uniform_raw_districts") }}
        where
            state_postal_code
            in (select state from {{ ref("l2_manual_district_assignments") }})
    )
select assignments.state, assignments.county, assignments.city
from {{ ref("l2_manual_district_assignments") }} as assignments
left join
    voter_places
    on voter_places.state_postal_code = assignments.state
    and (assignments.county is null or assignments.county = voter_places.county)
    and (assignments.city is null or assignments.city = voter_places.city)
where
    assignments.precinct is null
    and (assignments.county is not null or assignments.city is not null)
    and voter_places.state_postal_code is null
