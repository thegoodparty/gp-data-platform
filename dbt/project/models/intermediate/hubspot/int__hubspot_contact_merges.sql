-- One row per HubSpot contact merged into another. Airbyte never re-extracts a
-- merged-away contact, so its last extract stays in staging looking live; the
-- survivor's merged_object_ids is the only sign it is gone.
with
    listed as (
        select
            id as listing_contact_id,
            explode(split(merged_object_ids, ';')) as merged_contact_id
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),

    -- A survivor that kept its id lists itself.
    merges as (
        select listing_contact_id, merged_contact_id
        from listed
        where merged_contact_id != listing_contact_id
    )

select
    m.merged_contact_id,
    -- The lister not itself merged away. After a chain (a into b, then b into c)
    -- b's frozen row still lists a; null when c's list did not carry a forward.
    max(
        case when chained.merged_contact_id is null then m.listing_contact_id end
    ) as surviving_contact_id
from merges as m
left join merges as chained on chained.merged_contact_id = m.listing_contact_id
group by m.merged_contact_id
