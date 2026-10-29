-- An organization's uploaded contacts, keyed like serve_agent_constituents and
-- with no names, emails or phones, so an agent can join the two.
with
    overlay as (
        select organization_slug, id, is_new, email
        from {{ ref("gp_api_contact_overlay_dev") }}
    ),

    constituents as (
        select id, `LALVOTERID`, individual_id from {{ ref("gp_api_constituents") }}
    )

select
    overlay.organization_slug,
    -- Matches serve_agent_constituents' key for a known person. A new person
    -- has no L2 row, so their gp-api id is hashed under a prefix of its own.
    sha2(
        case
            when constituents.`LALVOTERID` is not null
            then constituents.`LALVOTERID`
            when constituents.individual_id is not null
            then concat('commercial:', constituents.individual_id)
            else concat('upload:', overlay.id)
        end,
        256
    ) as voter_key,
    overlay.is_new,
    true as on_uploaded_list,
    overlay.email is not null as has_email
from overlay
left join constituents on overlay.id = constituents.id
