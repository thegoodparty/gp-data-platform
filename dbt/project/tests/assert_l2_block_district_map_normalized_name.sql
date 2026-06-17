-- Binds the normalization: every district_name on the map must already equal
-- its own normalization (idempotent), proving normalize_l2_district_name was
-- applied so downstream joins to the resolver's normalized_district_name align.
select block_geoid, district_type, district_name
from {{ ref("int__l2_block_district_map") }}
where district_name != {{ normalize_l2_district_name("district_name") }}
