-- The race mart must keep recently-passed races (2-month grace period):
-- the campaign plan reads races after election day, so a future-only
-- window breaks it the morning after an election. Local elections run
-- year-round, so an empty past-race set means the window regressed.
select count(*) as past_race_count
from {{ ref("m_election_api__race") }}
where election_date < current_date()
having count(*) = 0
