{{ config(severity="error", error_if=">0") }}

-- A special election and a regular election for the same office in the same
-- year must resolve to distinct gp_election_ids. Anything > 0 means the
-- is_special partitioning in int__civics_candidacy_stage_ddhq broke and
-- specials are colliding back into the regular hash.
with
    elections as (
        select
            gp_election_id,
            official_office_name,
            state_postal_code,
            year(election_date) as election_year,
            max(election_stage like '%special%') as is_special
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
        group by
            gp_election_id, official_office_name, state_postal_code, year(election_date)
    )

select official_office_name, state_postal_code, election_year
from elections
group by official_office_name, state_postal_code, election_year
having
    bool_or(is_special)
    and bool_or(not is_special)
    and count(distinct gp_election_id) < 2
