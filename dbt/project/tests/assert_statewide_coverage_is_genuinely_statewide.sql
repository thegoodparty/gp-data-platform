-- DATA-1986: statewide (district_type='State') coverage must be limited to
-- genuinely statewide, contestable offices — positions whose BR geography is
-- the whole state (mtfcc='G4000') and that are not judicial retention seats.
--
-- The LLM BR Office <-> L2 District matcher over-assigns l2_district_type='State'
-- to non-statewide positions (district/circuit/appellate judges, state
-- legislative districts, water/community districts); the gate in
-- int__zip_code_to_br_office drops those. Human-curated statewide exceptions in
-- the override seed are exempt (they carry their own intentional State mapping).
--
-- Each row returned is a matcher-path 'State' position that should have been
-- gated out.
select z.br_database_id, pos.name, pos.mtfcc, pos.is_retention
from {{ ref("int__zip_code_to_br_office") }} as z
inner join
    {{ ref("stg_airbyte_source__ballotready_api_position") }} as pos
    on z.br_database_id = pos.database_id
where
    z.district_type = 'State'
    and z.br_database_id not in (
        select br_database_id
        from {{ ref("l2_br_match_overrides") }}
        where br_database_id is not null
    )
    and (pos.mtfcc <> 'G4000' or coalesce(pos.is_retention, false))
