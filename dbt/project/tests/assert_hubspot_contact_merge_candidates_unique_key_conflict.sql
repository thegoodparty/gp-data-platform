-- A merge keeps the primary's value of a unique property, so the secondary's is
-- gone once the pair merges. The flag has to cover every pair where that would
-- happen: the two rows holding different values, both non-null.
with
    candidates as (select * from {{ ref("hubspot_contact_merge_candidates") }}),

    primaries as (
        select gp_person_id, br_candidacy_id as primary_br_candidacy_id
        from candidates
        where merge_role = 'primary'
    )

select
    c.gp_person_id,
    c.hs_contact_id,
    c.merge_role,
    c.br_candidacy_id,
    p.primary_br_candidacy_id,
    c.has_unique_key_conflict
from candidates as c
left join primaries as p using (gp_person_id)
where
    -- Null on the primary and set on every secondary, like matched_on.
    (c.merge_role = 'primary') != (c.has_unique_key_conflict is null)
    or (
        c.has_unique_key_conflict != coalesce(
            c.br_candidacy_id is not null
            and p.primary_br_candidacy_id is not null
            and c.br_candidacy_id != p.primary_br_candidacy_id,
            false
        )
    )
