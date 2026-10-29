-- An organization's uploaded contacts, keyed like win_agent_voters and with no
-- names, emails or phones, so the Win agent can join the two.
with
    overlay as (
        select organization_slug, id, is_new, email
        from {{ ref("gp_api_contact_overlay_dev") }}
    ),

    voters as (select id, `LALVOTERID` from {{ ref("gp_api_voters") }})

select
    overlay.organization_slug,
    -- Matches win_agent_voters' key for a known voter. A new person has no L2
    -- row, so their gp-api id is hashed under a prefix of its own.
    sha2(
        coalesce(voters.`LALVOTERID`, concat('upload:', overlay.id)), 256
    ) as voter_key,
    overlay.is_new,
    true as on_uploaded_list,
    overlay.email is not null as has_email
from overlay
left join voters on overlay.id = voters.id
