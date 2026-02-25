-- Every candidate with a 2026+ election must have at least one candidacy.
-- Scoped to 2026+ because legacy HubSpot candidates may exist without
-- candidacy records. Failures indicate an ID-generation mismatch between
-- candidate and candidacy intermediate models.
{{
    config(
        severity="error",
        warn_if=">15",
        error_if=">50",
    )
}}

select c.gp_candidate_id
from {{ ref("candidate") }} c
where
    c.gp_candidate_id in (
        select distinct gp_candidate_id
        from {{ ref("candidacy") }}
        where coalesce(general_election_date, primary_election_date) >= '2026-01-01'
    )
    and c.gp_candidate_id
    not in (select distinct gp_candidate_id from {{ ref("candidacy") }})
