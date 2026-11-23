-- The app writes to the contact a user's meta_data points at, so a person
-- holding an app-linked contact must rank the most recently active one
-- primary; its email is the account's login. Compared against the person's
-- max, not pairwise: two app-linked contacts can share an activity timestamp.
with
    app_linked as (
        select gp_person_id, max(app_user_last_active_at) as latest_app_activity
        from {{ ref("hubspot_contact_merge_candidates") }}
        where is_app_linked
        group by gp_person_id
    )

select c.gp_person_id, c.hs_contact_id, c.is_app_linked, c.app_user_last_active_at
from {{ ref("hubspot_contact_merge_candidates") }} as c
inner join app_linked as a using (gp_person_id)
where
    c.merge_role = 'primary'
    and c.app_user_last_active_at is distinct from a.latest_app_activity
