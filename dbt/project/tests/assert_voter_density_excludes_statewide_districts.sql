-- The heat map must never publish a statewide district: a whole state's voter
-- file is meaningless as a "where they live" surface, and the app never requests
-- one.
--
-- This holds today by the bridge's grain rather than by a filter. The bridge
-- carries no bare 'State' type (measured: zero rows), because statewide
-- associations are unioned in separately downstream, so statewide districts have
-- no bridge rows and cannot reach the marts. A `type <> 'State'` predicate in the
-- cell-counts model would therefore match nothing while reading as an active
-- guarantee, which is worse than asserting it here.
--
-- Checked against the district mart's own `type`, which is where statewide is
-- authoritatively recorded (m_people_api__districtstats identifies statewide the
-- same way). Runs at the meta mart's district grain, so it is a small join, not a
-- scan of the published cells. Error severity, not warn: this is the feature's
-- scope contract, and a violation means the bridge started carrying statewide
-- associations and the marts silently began publishing them.
select meta.district_id, meta.resolution, district.type
from {{ ref("m_people_api__district_voter_density_meta") }} as meta
inner join
    {{ ref("m_people_api__district") }} as district on district.id = meta.district_id
where district.type = 'State'
