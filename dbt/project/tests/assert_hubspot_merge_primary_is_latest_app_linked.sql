-- A gp_api user's writes land on the contact its meta_data points at, so a person
-- holding an app-linked contact must merge into one of them, and the most recently
-- active one, whose email is the account's login email.
--
-- Checked against the person's latest app-linked activity rather than pairwise, so a
-- person holding two app-linked contacts is not a failure when the ranking legitimately
-- picks one of them: only a primary that is not app-linked, or one that is older than
-- another app-linked contact, fails.
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
    and (
        not c.is_app_linked
        or c.app_user_last_active_at is distinct from a.latest_app_activity
    )
