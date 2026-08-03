{{
    config(
        materialized="table",
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "people_api", "voter_h3", "voter_density"],
    )
}}

/*
Voter → H3 binning for the voter-density heat map (see
packages/people-api/docs/voter-density-heatmap-handoff.md §3.1).

Parses each voter's residence lat/lng and bins it to H3 at the published
resolutions. One row per voter; one H3 column per resolution. Downstream
m_people_api__district_voter_density explodes these columns, joins to the
district↔voter bridge, aggregates, and K-suppresses.

Key derivation note (handoff §4): the join key back to
m_people_api__districtvoter.voter_id is the voter's salted-uuid `id` from
m_people_api__voter (generate_salted_uuid([LALVOTERID], salt='l2')), NOT the
raw LALVOTERID. We therefore source `id` directly from the voter mart and
expose it as `voter_id` so the bridge joins cleanly — we never re-mint an id.

H3 UDFs: these are Databricks built-in H3 SQL functions
(h3_longlatash3 takes (lng, lat, res) — longitude first). No prior model in
this repo used H3; the arg order and centroid handling here are the reference
for future H3 models (handoff §3.1 / §2.3).

`try_cast` (not `cast`) so a non-numeric lat/lng yields NULL and is dropped
rather than failing the model. Keep `latlong_accuracy` for the coverage/meta
model and the Phase-0 rooftop-vs-zip-centroid analysis (handoff §6.2).
*/
with
    parsed as (
        select
            voter.id as voter_id,
            voter.`LALVOTERID` as lalvoterid,
            voter.`State` as state,
            try_cast(voter.`Residence_Addresses_Latitude` as double) as lat,
            try_cast(voter.`Residence_Addresses_Longitude` as double) as lng,
            voter.`Residence_Addresses_LatLongAccuracy` as latlong_accuracy
        from {{ ref("m_people_api__voter") }} as voter
    )

select
    voter_id,
    lalvoterid,
    state,
    lat,
    lng,
    latlong_accuracy,
    -- One column per published resolution. Add/remove to match the §6
    -- office-level → resolution policy once Phase 0 confirms it.
    h3_longlatash3(lng, lat, 7) as h3_r7,
    h3_longlatash3(lng, lat, 8) as h3_r8,
    h3_longlatash3(lng, lat, 9) as h3_r9
from parsed
where
    lat is not null
    and lng is not null
    -- US bounding-box sanity filter drops obviously-bad geocodes (0/0, swapped
    -- signs, non-US). Tighten per state if needed (handoff §3.1).
    and lat between 17.0 and 72.0
    and lng between -180.0 and -64.0
