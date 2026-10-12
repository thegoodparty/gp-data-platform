{{
    config(
        materialized="table",
        tags=["intermediate", "voter_h3", "voter_density", "monthly"],
    )
}}

/*
Voter → H3 binning for the voter-density heat map.

One row per geocoded voter, one H3 column per published resolution. The
resolution list comes from the voter_density_h3_resolutions var so the policy
lives in one place.

The join key back to int__l2_district_voters.voter_id is the voter's
salted-uuid `id` from gp_api_voters, NOT the raw LALVOTERID — we source
`id` directly rather than re-minting it.

h3_longlatash3 takes (lng, lat, res) — longitude FIRST. No prior model in this
repo used H3, so this is the reference for the arg order.

`try_cast` (not `cast`) so a non-numeric lat/lng yields NULL and is dropped
rather than failing the model.
*/
with
    parsed as (
        select
            voter.id as voter_id,
            try_cast(voter.`Residence_Addresses_Latitude` as double) as lat,
            try_cast(voter.`Residence_Addresses_Longitude` as double) as lng
        from {{ ref("gp_api_voters") }} as voter
    )

select
    voter_id,
    {%- for r in var("voter_density_h3_resolutions") %}
        h3_longlatash3(lng, lat, {{ r }}) as h3_r{{ r }}{{ "," if not loop.last }}
    {%- endfor %}
from parsed
where
    lat is not null
    and lng is not null
    -- Bounding box drops obviously-bad geocodes (0/0, swapped signs, non-US).
    -- Measured against prod it currently removes zero additional rows beyond the
    -- null check, so treat it as defence-in-depth rather than active cleanup.
    and lat between 17.0 and 72.0
    and lng between -180.0 and -64.0
