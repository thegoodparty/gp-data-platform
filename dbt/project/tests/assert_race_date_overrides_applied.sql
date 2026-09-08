-- Every date override must name a race that exists and carries the corrected
-- values. A key that misses (a retired BallotReady race id, or a cycle
-- BallotReady has not published yet) leaves the seed a silent no-op that looks
-- like a fix.
select overrides.br_race_database_id
from {{ ref("br_race_date_overrides") }} as overrides
left join
    {{ ref("int__enhanced_race") }} as races
    on races.br_database_id = overrides.br_race_database_id
    and cast(races.election_date as date) = overrides.election_date
    and races.filing_date_start = overrides.filing_date_start
    and races.filing_date_end = overrides.filing_date_end
where races.id is null
