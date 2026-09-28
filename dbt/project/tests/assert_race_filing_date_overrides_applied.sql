-- Every filing window override must match at least one race in the mart and be
-- the value that race carries. A key that misses (a state, level, or partisan
-- type that names nothing, or an election date past the mart's serving window)
-- leaves the seed a silent no-op that looks like a fix.
select
    overrides.state,
    overrides.election_date,
    overrides.position_level,
    overrides.partisan_type
from {{ ref("election_api_race_filing_date_overrides") }} as overrides
left join
    {{ ref("m_election_api__race") }} as races
    on races.state = overrides.state
    and cast(races.election_date as date) = overrides.election_date
    and races.position_level = overrides.position_level
    and races.partisan_type = overrides.partisan_type
    and races.filing_date_start = overrides.filing_date_start
    and races.filing_date_end = overrides.filing_date_end
where races.id is null
