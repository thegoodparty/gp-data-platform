/*
Voter-density coverage bookkeeping — loaded to people-api Postgres
green."DistrictVoterDensityMeta". See handoff §3.3 / §7.

One row per (district_id, resolution). The app uses `coverage` to decide whether
the map is trustworthy enough to render (it hides the map below a threshold) and
to draw the legend. Suppressed voters still count toward the coverage denominator
but never produce a rendered cell (privacy contract §2.2).

Output schema (must match handoff §7 green."DistrictVoterDensityMeta"):
    - district_id:      uuid   (== District.id — §4)
    - resolution:       int
    - coverage:         double (rendered_voters / total_voters ∈ [0,1] — §3.3)
    - min_cell_count:   int    (the K used for this build)
    - total_voters:     int    (district voters at this resolution grain)
    - geocoded_voters:  int    (voters with a usable H3)
    - rendered_voters:  int    (sum of voter_count over non-suppressed cells)
    - suppressed_cells: int    (cells dropped by K-anonymity)
    - state:            string (loaded into the "State" USState enum)
    - updated_at:       timestamp

Full rebuild each run (materialized table): the grain is tiny (districts ×
resolutions) and coverage must be recomputed against the current suppression, so
there is no incremental benefit and a stale denominator would mislead the app.
*/
{{
    config(
        materialized="table",
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["mart", "people_api", "district_voter_density_meta", "voter_density"],
    )
}}

with
    -- District voters, excluding statewide districts (same scope as the density
    -- mart, handoff §3.2).
    district_voter as (
        select districtvoter.district_id, districtvoter.voter_id, districtvoter.state
        from {{ ref("m_people_api__districtvoter") }} as districtvoter
        where districtvoter.type <> 'State'
    ),

    -- Explode every district voter across all published resolutions. LEFT join
    -- to int__people_api__voter_h3 so non-geocoded voters are retained (they
    -- count toward total_voters but have a NULL h3_index). Building the map from
    -- the (possibly NULL) voter_h3 columns still yields one row per resolution.
    exploded as (
        select
            district_voter.district_id,
            district_voter.voter_id,
            district_voter.state,
            resolution,
            h3_index
        from district_voter
        left join
            {{ ref("int__people_api__voter_h3") }} as voter_h3
            on voter_h3.voter_id = district_voter.voter_id
        lateral view
            explode(map(7, voter_h3.h3_r7, 8, voter_h3.h3_r8, 9, voter_h3.h3_r9))
            as resolution, h3_index
    ),

    -- Per (district, resolution): total voters and geocoded voters. One row per
    -- (voter, resolution) in `exploded`, so count(*) is the district voter count.
    base as (
        select
            district_id,
            resolution,
            any_value(state) as state,
            count(*) as total_voters,
            count(h3_index) as geocoded_voters
        from exploded
        group by district_id, resolution
    ),

    -- Pre-suppression per-cell counts, to tally cells dropped by K-anonymity.
    pre_suppression as (
        select district_id, resolution, h3_index, count(*) as voter_count
        from exploded
        where h3_index is not null
        group by district_id, resolution, h3_index
    ),
    suppressed as (
        select district_id, resolution, count(*) as suppressed_cells
        from pre_suppression
        where voter_count < {{ var("voter_density_k", 10) }}
        group by district_id, resolution
    ),

    -- Post-suppression totals from the published density mart.
    rendered as (
        select
            district_id,
            resolution,
            sum(voter_count) as rendered_voters,
            count(*) as rendered_cells
        from {{ ref("m_people_api__district_voter_density") }}
        group by district_id, resolution
    )

select
    base.district_id,
    base.resolution,
    -- Fraction of the district's voters represented by rendered (non-suppressed)
    -- cells. The app hides the map below a threshold (handoff §3.3 / §8).
    case
        when base.total_voters > 0
        then coalesce(rendered.rendered_voters, 0) / cast(base.total_voters as double)
        else 0
    end as coverage,
    {{ var("voter_density_k", 10) }} as min_cell_count,
    base.total_voters,
    base.geocoded_voters,
    coalesce(rendered.rendered_voters, 0) as rendered_voters,
    coalesce(suppressed.suppressed_cells, 0) as suppressed_cells,
    base.state,
    current_timestamp() as updated_at
from base
left join rendered on base.district_id = rendered.district_id and base.resolution = rendered.resolution
left join suppressed on base.district_id = suppressed.district_id and base.resolution = suppressed.resolution
