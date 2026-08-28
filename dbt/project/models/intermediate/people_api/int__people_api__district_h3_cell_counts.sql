{{
    config(
        materialized="table",
        tags=[
            "intermediate",
            "people_api",
            "district_h3_cell_counts",
            "voter_density",
            "monthly",
        ],
    )
}}

/*
Pre-suppression H3 cell counts per district — the single shared input to both
voter-density marts.

Grain: (district_id, resolution, h3_index), where h3_index is NULL for the
district's non-geocoded voters. Keeping that NULL group is what lets the meta
mart derive total_voters, geocoded_voters and suppressed_cells from this table
alone, so the district↔voter join and the resolution fan-out happen exactly
once for the whole feature rather than once per mart.

No K-suppression here: this is deliberately the PRE-suppression grain. The
density mart applies `>= K`, the meta mart counts what fell below it. Both read
the same numbers, so coverage can never describe a different cell population
than the map actually draws.

`stack` rather than `explode(map(...))`: it produces the same rows without
building and tearing down a map object per input row.
*/
with
    -- No statewide filter here. The bridge carries no bare 'State' type at all
    -- (measured: zero rows), because statewide associations are unioned in
    -- separately downstream rather than living in the bridge, so statewide
    -- districts have no bridge rows and cannot produce cells. A
    -- `type <> 'State'` predicate would read as an active guarantee while
    -- matching nothing. The guarantee is asserted instead, against the district
    -- mart's own type, by assert_voter_density_excludes_statewide_districts.
    district_voter as (
        select districtvoter.district_id, districtvoter.voter_id, districtvoter.state
        from {{ ref("m_people_api__districtvoter") }} as districtvoter
    ),

    -- LEFT join so non-geocoded voters survive: they carry a NULL h3_index and
    -- count toward total_voters without ever producing a cell.
    exploded as (
        select district_voter.district_id, district_voter.state, resolution, h3_index
        from district_voter
        left join
            {{ ref("int__people_api__voter_h3") }} as voter_h3
            on voter_h3.voter_id = district_voter.voter_id
        lateral view
            stack(
                {{ var("voter_density_h3_resolutions") | length }}
                {%- for r in var("voter_density_h3_resolutions") %}
                    , {{ r }}, voter_h3.h3_r{{ r }}
                {%- endfor %}
            ) as resolution,
            h3_index
    )

select
    district_id,
    resolution,
    h3_index,
    any_value(state) as state,
    count(*) as voter_count
from exploded
group by district_id, resolution, h3_index
