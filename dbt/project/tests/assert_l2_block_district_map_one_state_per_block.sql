-- A census block lives in exactly one state (block_geoid[:2] = state FIPS), so
-- more than one state_postal_code on a block_geoid is an upstream L2
-- geocode/state mismatch. Fail so it is fixed upstream, not silently blended
-- across states by the voters_in_block window.
select block_geoid, count(distinct state_postal_code) as n_states
from {{ ref("int__l2_block_district_map") }}
group by block_geoid
having count(distinct state_postal_code) > 1
