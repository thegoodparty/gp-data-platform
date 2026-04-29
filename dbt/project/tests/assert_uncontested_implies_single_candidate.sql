{{
    config(
        severity="warn",
        warn_if=">10",
        error_if=">100",
    )
}}

-- A race flagged as uncontested should have exactly one candidate. Multiple
-- candidates on an "uncontested" race is something a campaign operative
-- would immediately spot as wrong. Soft warn — DDHQ occasionally reports
-- a race as uncontested even when 2+ candidates appear (write-ins, etc).
select gp_election_stage_id, count(*) as candidate_count
from {{ ref("candidacy_stage") }}
where is_uncontested
group by gp_election_stage_id
having count(*) > 1
