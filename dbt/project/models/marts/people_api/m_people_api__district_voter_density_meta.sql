/*
Voter-density coverage bookkeeping — loaded to people-api Postgres
green."DistrictVoterDensityMeta". One row per (district_id, resolution).

The app uses `coverage` to decide whether the map is trustworthy enough to render
(it hides the map below a threshold) and to draw the legend. Suppressed voters
still count toward the coverage denominator but never produce a rendered cell.

Every measure is a reduction over int__people_api__district_h3_cell_counts, the
same pre-suppression table the density mart filters. That is deliberate: deriving
coverage from a second, independently-computed population is how the denominator
silently stops describing the map it is reported against.

total_voters and geocoded_voters are identical across resolutions — a voter's H3
cell exists at every resolution or at none — but are published per resolution
because the app reads them keyed by (district, resolution) alongside coverage.

Column semantics are documented in m_people_api.yaml.
*/
{{
    config(
        on_schema_change="fail",
        tags=["mart", "people_api", "district_voter_density_meta", "voter_density"],
    )
}}

with
    cells as (
        select
            district_id,
            resolution,
            any_value(state) as state,
            sum(voter_count) as total_voters,
            sum(
                case when h3_index is not null then voter_count else 0 end
            ) as geocoded_voters,
            sum(
                case
                    when
                        h3_index is not null
                        and voter_count >= {{ var("voter_density_k") }}
                    then voter_count
                    else 0
                end
            ) as rendered_voters,
            count_if(
                h3_index is not null and voter_count < {{ var("voter_density_k") }}
            ) as suppressed_cells
        from {{ ref("int__people_api__district_h3_cell_counts") }}
        group by district_id, resolution
    )

select
    district_id,
    resolution,
    -- Fraction of the district's voters represented by rendered (non-suppressed)
    -- cells. total_voters is a group count, so it is always >= 1.
    rendered_voters / total_voters as coverage,
    {{ var("voter_density_k") }} as min_cell_count,
    total_voters,
    geocoded_voters,
    rendered_voters,
    suppressed_cells,
    state,
    current_timestamp() as updated_at
from cells
