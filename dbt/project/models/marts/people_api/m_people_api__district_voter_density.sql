/*
Voter-density heat map mart — loaded to people-api Postgres green."DistrictVoterDensity".

Aggregated, K-anonymized, H3-binned voter *residence density* per district. Each
row is an H3 cell with a voter count and the deterministic H3 cell centroid —
never a voter, address, or the mean of voter positions. Two different voter
distributions in the same cell publish the identical point.

Column semantics are documented in m_people_api.yaml; the grain is one row per
(district_id, resolution, h3_index) surviving suppression.

Materialization — K-anonymity correctness: each district must be recomputed AS A
WHOLE, never merged cell-by-cell, or suppressed cells leak as a diff. A
`merge`/`delete+insert` on the (district_id, resolution, h3_index) key upserts
surviving cells but CANNOT delete a cell that dropped below K since the last run;
the stale row and its old count would linger. Full rebuild guarantees a cell that
falls under K simply disappears. If volume later makes this too slow, move to a
whole-district `replace_where` delete+insert, NOT a plain cell-key merge.
*/
{{
    config(
        tags=[
            "mart",
            "people_api",
            "district_voter_density",
            "voter_density",
            "monthly",
        ],
    )
}}

with
    cells as (
        select
            district_id,
            resolution,
            h3_index,
            state,
            voter_count,
            split(
                regexp_extract(h3_centeraswkt(h3_index), 'POINT\\(([^)]+)\\)', 1), ' '
            ) as centroid
        from {{ ref("int__people_api__district_h3_cell_counts") }}
        where h3_index is not null and voter_count >= {{ var("voter_density_k") }}
    )

select
    district_id,
    resolution,
    h3_h3tostring(h3_index) as h3_index,
    -- Deterministic H3 cell centroid, parsed out of WKT 'POINT(lng lat)' once.
    cast(centroid[1] as double) as lat,
    cast(centroid[0] as double) as lng,
    voter_count,
    state,
    current_timestamp() as updated_at
from cells
