-- WARN: a candidate_code mapping to more than one distinct person (gp_candidate_id)
-- is a
-- cross-person collision -- ambiguous lead identity on match-back. ~0.2% of codes
-- collide
-- today, and most apparent collisions are upstream entity-resolution duplicates of
-- one person
-- (see .tickets/DATA-1523/46_layer1_models_design.md §11.1). Severity is warn until
-- the ER
-- duplicates are cleaned upstream; promote to error after that. (HubSpot leads are
-- identity-grained, so this checks the code alone -- not code + year + stage like
-- TechSpeed.)
{{ config(severity="warn") }}
select candidate_code, count(distinct gp_candidate_id) as n_distinct_people
from {{ ref("candidacy_hubspot") }}
where candidate_code is not null
group by candidate_code
having count(distinct gp_candidate_id) > 1
