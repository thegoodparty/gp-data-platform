-- exact_match lets the merge job run a pair unattended, so the one thing it must
-- never do is call a pair exact when the last names disagree: the identity
-- evidence (a shared native id or phone) survives a household or a mis-stamped
-- id, and the surname is what rules those out. A row comes back when the flag is
-- true and either the surname differs or the evidence says the names do not
-- match. Asserted one way round on purpose, so the test states the rule rather
-- than re-deriving the model's CASE.
with
    candidates as (select * from {{ ref("hubspot_contact_merge_candidates") }}),

    primaries as (
        select gp_person_id, lower(trim(last_name)) as primary_last_name
        from candidates
        where merge_role = 'primary'
    )

select
    c.gp_person_id,
    c.hs_contact_id,
    c.matched_on,
    c.match_evidence,
    c.last_name,
    p.primary_last_name
from candidates as c
left join primaries as p using (gp_person_id)
where
    c.exact_match
    and (
        c.match_evidence in ('name_differs', 'name_incomplete')
        or coalesce(lower(trim(c.last_name)) != p.primary_last_name, true)
    )
