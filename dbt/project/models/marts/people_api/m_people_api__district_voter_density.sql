/*
Voter-density heat map mart — loaded to people-api Postgres green."DistrictVoterDensity".
See packages/people-api/docs/voter-density-heatmap-handoff.md §3.2 / §7.

Aggregated, K-anonymized, H3-binned voter *residence density* per district. Each
row is an H3 cell with a voter count and the deterministic H3 cell centroid —
never a voter, address, or the mean of voter positions (privacy contract §2).

Pipeline: explode the per-resolution H3 columns from int__people_api__voter_h3
into (voter, resolution, h3_index) rows, join to the district↔voter bridge
(m_people_api__districtvoter), exclude 'State' districts, group by
district_id + resolution + h3_index, K-suppress (var voter_density_k, default 10),
and emit the H3 cell centroid.

Output schema (must match handoff §7 green."DistrictVoterDensity"):
    - district_id: uuid   (== District.id; sourced from the bridge, never re-minted — §4)
    - resolution:  int    (H3 resolution of this row)
    - h3_index:    string  (h3_h3tostring(h3) — opaque to the app)
    - lat:         double  (H3 cell centroid latitude — §2.3)
    - lng:         double  (H3 cell centroid longitude)
    - voter_count: int     (count in cell; always >= K — §2.2)
    - state:       string  (two-letter state; loaded into the "State" USState enum)
    - updated_at:  timestamp (mart run time)

Materialization (handoff §3.2 — K-anonymity correctness): the privacy contract
requires each district to be recomputed AS A WHOLE, never merged cell-by-cell,
"or suppressed cells would leak as a diff". A `merge`/`delete+insert` on the
(district_id, resolution, h3_index) key upserts surviving cells but CANNOT delete
a cell that dropped below K since the last run — the stale row (and its old
count) would linger and leak. We therefore full-rebuild the table each run, which
trivially satisfies "whole-district recomputation" and guarantees a cell that
falls under K simply disappears. The grain is post-suppression cells per district
× resolution — orders of magnitude smaller than the voter file — so a full
rebuild is cheap. If volume later makes this too slow, move to a whole-district
delete+insert (Databricks `replace_where` with the changed-district predicate),
NOT a plain cell-key merge. See the PR body's incremental/loader note.
*/
{{
    config(
        materialized="table",
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["mart", "people_api", "district_voter_density", "voter_density"],
    )
}}

with
    -- District voters, excluding statewide ('State') districts: the voter file
    -- for a whole state is enormous and meaningless as a "where they live"
    -- surface, and the app never requests a State district (handoff §3.2).
    district_voter as (
        select districtvoter.district_id, districtvoter.voter_id, districtvoter.state
        from {{ ref("m_people_api__districtvoter") }} as districtvoter
        where districtvoter.type <> 'State'
    ),

    -- Unpivot the per-resolution H3 columns into (voter, resolution, h3_index)
    -- rows so one grouping handles every resolution. h3_index is the H3 BIGINT
    -- id here; it is converted to the opaque string only on final output.
    exploded as (
        select district_voter.district_id, district_voter.state, resolution, h3_index
        from district_voter
        inner join
            {{ ref("int__people_api__voter_h3") }} as voter_h3
            on voter_h3.voter_id = district_voter.voter_id
        lateral view
            explode(
                map(7, voter_h3.h3_r7, 8, voter_h3.h3_r8, 9, voter_h3.h3_r9)
            ) as resolution,
            h3_index
        where h3_index is not null
    ),

    -- Pre-suppression per-cell aggregation (full rebuild — see the header note on
    -- why this is not incremental).
    agg as (
        select
            district_id,
            resolution,
            h3_index,
            any_value(state) as state,
            count(*) as voter_count
        from exploded
        group by district_id, resolution, h3_index
    ),

    -- K-anonymity suppression (privacy contract §2.2) + deterministic centroid.
    suppressed as (
        select
            district_id,
            resolution,
            h3_index,
            state,
            voter_count,
            -- Deterministic H3 cell centroid as WKT 'POINT(lng lat)'
            -- (handoff §2.3). Two different voter distributions in the same cell
            -- produce the identical published point.
            h3_centeraswkt(h3_index) as centroid_wkt
        from agg
        where voter_count >= {{ var("voter_density_k", 10) }}
    )

select
    district_id,
    resolution,
    h3_h3tostring(h3_index) as h3_index,
    cast(
        split(regexp_extract(centroid_wkt, 'POINT\\(([^)]+)\\)', 1), ' ')[1] as double
    ) as lat,
    cast(
        split(regexp_extract(centroid_wkt, 'POINT\\(([^)]+)\\)', 1), ' ')[0] as double
    ) as lng,
    voter_count,
    state,
    current_timestamp() as updated_at
from suppressed
