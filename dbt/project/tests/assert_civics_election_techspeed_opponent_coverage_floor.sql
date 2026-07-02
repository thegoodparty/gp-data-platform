-- Regression guard on the TechSpeed opponent count (DATA-1938).
-- int__civics_election_techspeed must CARRY the opponent count derived from
-- the TechSpeed source (number_candidates - 1), not hardcode number_of_opponents
-- to null. The hardcoded null starved int__civics_viability_scoring of the
-- log_n_losers feature, forcing solo-ingested TechSpeed candidacies onto the
-- weaker viabilitynoopponentdata fallback.
--
-- Fails when the populated share drops below the floor, or when the model is
-- empty (count(*) = 0, a total upstream break). A share floor, not a fixed
-- count, so TechSpeed volume changes never trip it on their own. TechSpeed's
-- number_candidates is ~fully populated at source, so a real drop here means a
-- revert to the hardcoded null or a break in the source mapping. `* 1.0` keeps
-- the ratio an explicit float per the sibling coverage tests, and
-- nullif(count(*), 0) guards the empty model from an ANSI divide-by-zero.
select count(*) as total_ts_elections, count(number_of_opponents) as populated_opponents
from {{ ref("int__civics_election_techspeed") }}
having count(*) = 0 or count(number_of_opponents) * 1.0 / nullif(count(*), 0) < 0.90
