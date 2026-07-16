-- General rows are hand-seeded, so recompute the fixed federal rule
-- (2 U.S.C. 7: the Tuesday after the first Monday in November) and fail on
-- any divergence -- a seed typo must not serve a wrong general-election
-- day. next_day(d, 'MON') is strictly after d, so anchoring at Oct 31
-- keeps a Nov-1 Monday resolving to Nov 1 (same expression as the
-- collision guard in int__election_calendar_primary_ballotready).
select state, election_date
from {{ ref("m_election_api__election_calendar") }}
where
    election_code = 'General'
    and election_date != date_add(
        next_day(make_date(year(election_date), 11, 1) - interval 1 day, 'MON'), 1
    )
