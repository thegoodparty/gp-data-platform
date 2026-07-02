{{ config(materialized="view") }}

-- Serve per-official display surface: one row per resolved serve official with the
-- population and registered voters of the jurisdiction they serve. Local officials
-- join district_census_stats on (state, type, normalized name); statewide officials
-- read the district_type='State' row by state. No fan-out: one row per official.
-- Officials that bind nothing get has_census_match=false and null constituents (a
-- documented gap, not a defect); requires_review rows are flagged, never filtered.
with
    resolver as (
        select *
        from {{ ref("int__serve_district_resolution") }}
        where resolution_path != 'unresolved'
    ),

    stats as (select * from {{ ref("district_census_stats") }})

select
    r.organization_slug,
    r.user_id,
    r.state,
    r.l2_district_type,
    r.normalized_district_name,
    r.is_statewide,
    r.resolution_path,
    r.requires_review,
    r.is_geo_seat,
    r.in_people_served_cohort,
    coalesce(s.district_population, sw.district_population) as constituents,
    coalesce(s.registered_voters, sw.registered_voters) as registered_voters,
    coalesce(s.district_population, sw.district_population)
    - coalesce(s.registered_voters, sw.registered_voters) as constituents_minus_voters,
    (
        s.state_postal_code is not null or sw.state_postal_code is not null
    ) as has_census_match
from resolver r
left join
    stats s
    on not r.is_statewide
    and r.state = s.state_postal_code
    and r.l2_district_type = s.district_type
    and r.normalized_district_name = s.district_name
    and s.district_type != 'State'
left join
    stats sw
    on r.is_statewide
    and r.state = sw.state_postal_code
    and sw.district_type = 'State'
