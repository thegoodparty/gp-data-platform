-- A block group's shares across one district_type sum to at most 1, so a state's local
-- districts of one type can never hold more ACS population than the state's block
-- groups do. A larger total means a weight or join is fanning out.
with
    districts as (
        select state_postal_code, district_type, sum(total_population) as allocated
        from {{ ref("int__district_acs_estimates") }}
        where district_type != 'State'
        group by state_postal_code, district_type
    ),

    states as (
        select
            states.place_name as state_postal_code, sum(acs.total_population) as total
        from {{ ref("stg_census_acs__geo_estimates") }} as acs
        inner join
            {{ ref("fips_codes") }} as states
            on left(acs.geoid, 2) = states.fips_code
            and states.level = 'state'
        where acs.summary_level = '150'
        group by states.place_name
    )

select districts.*, states.total
from districts
inner join states on districts.state_postal_code = states.state_postal_code
where districts.allocated > states.total + 1e-3
