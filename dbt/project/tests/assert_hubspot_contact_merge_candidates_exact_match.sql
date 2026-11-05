-- exact_match is the merge job's eligibility gate, so a wrong true merges a pair
-- nobody vouched for and a wrong false silently withholds one. A row comes back
-- when the flag disagrees with the rule, computed from the row itself and the
-- primary's last name.
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
    c.merge_role,
    c.matched_on,
    c.match_evidence,
    c.last_name,
    p.primary_last_name,
    c.exact_match
from candidates as c
left join primaries as p using (gp_person_id)
where
    -- Null on the primary and set on every secondary, like matched_on.
    (c.merge_role = 'primary') != (c.exact_match is null)
    or (
        c.merge_role = 'secondary'
        and c.exact_match != (
            (c.matched_on = 'native_ids' or c.match_evidence = 'same_phone')
            -- name_differs and name_incomplete are never exact, whatever else the
            -- pair shares.
            and c.match_evidence not in ('name_differs', 'name_incomplete')
            and coalesce(lower(trim(c.last_name)) = p.primary_last_name, false)
        )
    )
