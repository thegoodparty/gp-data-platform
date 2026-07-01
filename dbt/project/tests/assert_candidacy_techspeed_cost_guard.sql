{{ config(tags=["feed_invariant", "sales_reverse_etl"]) }}
-- DATA-1523 / DATA-2011 cutover guard (paid-enrichment cost guard).
-- The TechSpeed enrichment feed must NEVER include a candidacy TechSpeed already knows
-- (source_systems contains 'techspeed'); re-sending is paid re-enrichment. This
-- guards the
-- `AND NOT array_contains(cy.source_systems, 'techspeed')` predicate in
-- candidacy_techspeed.sql.
-- Zero tolerance: any row here is real money wasted.
select t.gp_candidacy_id
from {{ ref("candidacy_techspeed") }} as t
join {{ ref("candidacy") }} as c on t.gp_candidacy_id = c.gp_candidacy_id
where array_contains(c.source_systems, 'techspeed')
