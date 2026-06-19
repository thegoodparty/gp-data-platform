-- WARN: a tracking key (candidate_code + election_year + election_stage) that maps to
-- more
-- than one distinct person (gp_candidate_id) is a cross-person collision -- it would
-- misroute
-- a TechSpeed match-back or suppress a distinct person's send under the reverse-ETL
-- sent_log.
-- ~0.2% of codes collide today, and most apparent collisions are upstream
-- entity-resolution
-- duplicates of one person (see .tickets/DATA-1523/46_layer1_models_design.md §11.1).
-- Severity
-- is warn until the ER duplicates are cleaned upstream; promote to error after that.
{{ config(severity="warn") }}
select
    candidate_code,
    election_year,
    election_stage,
    count(distinct gp_candidate_id) as n_distinct_people
from {{ ref("candidacy_techspeed") }}
where candidate_code is not null
group by candidate_code, election_year, election_stage
having count(distinct gp_candidate_id) > 1
