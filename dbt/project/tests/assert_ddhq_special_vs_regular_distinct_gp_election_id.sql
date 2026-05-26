{{ config(severity="warn", warn_if=">5", error_if=">25") }}

-- A special election and a regular election for the same office in the same
-- year must resolve to distinct gp_election_ids. Anything > 0 means the
-- is_special partitioning in int__civics_candidacy_stage_ddhq broke and
-- specials are colliding back into the regular hash.
--
-- Per-row is_special is kept (not pre-aggregated per gp_election_id), so a
-- true collision shows up as two stage rows with different is_special values
-- sharing one gp_election_id, and bool_or at the outer (office, state, year)
-- group fires on the mismatch. Warn-only because real-world hits typically
-- reflect upstream ER clustering in int__civics_er_canonical_ids (which
-- overrides the hash-distinct gp_election_id), not a regression in the hash
-- itself.
select official_office_name, state_postal_code, election_year
from
    (
        select
            gp_election_id,
            official_office_name,
            state_postal_code,
            year(election_date) as election_year,
            election_stage like '%special%' as is_special
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
    ) as stages
group by official_office_name, state_postal_code, election_year
having
    bool_or(is_special)
    and bool_or(not is_special)
    and count(distinct gp_election_id) < 2
