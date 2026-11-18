-- One row per HubSpot contact merged into another. Two sources, because neither
-- alone is current: Airbyte rarely re-extracts a merged-away contact, so its last
-- extract stays in staging looking live, and the merge job's log knows about a
-- merge minutes after it happened but only for pairs the job itself ran. The log
-- closes the window before the survivor's merged_object_ids arrives on the next
-- sync.
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
        where merged_contact_id not in ('', listing_contact_id)
    ),

    from_hubspot as (
        select
            m.merged_contact_id,
            -- The lister not itself merged away. After a chain (a into b, then b
            -- into c) b's frozen row still lists a; null when c's list did not
            -- carry a forward.
            max(
                case
                    when chained.merged_contact_id is null then m.listing_contact_id
                end
            ) as surviving_contact_id
        from merges as m
        left join merges as chained on chained.merged_contact_id = m.listing_contact_id
        group by m.merged_contact_id
    ),

    -- The statuses that leave the secondary absorbed by another contact. A
    -- `failed` row, or a primary that went away, says nothing about the secondary.
    logged as (
        select
            secondary_id as merged_contact_id,
            survivor_id as surviving_contact_id,
            attempted_at
        from {{ ref("stg_airflow_source__hubspot_contact_merge_log") }}
        where status in ('merged', 'already_merged', 'id_changed', 'merged_elsewhere')
    ),

    from_log as (
        select merged_contact_id, surviving_contact_id
        from logged
        qualify
            row_number() over (
                partition by merged_contact_id order by attempted_at desc
            )
            = 1
    ),

    combined as (
        select
            coalesce(h.merged_contact_id, l.merged_contact_id) as merged_contact_id,
            -- HubSpot's own record wins where both know the secondary: it also
            -- reflects merges that happened outside this job.
            coalesce(
                h.surviving_contact_id, l.surviving_contact_id
            ) as surviving_contact_id
        from from_hubspot as h
        full outer join from_log as l on h.merged_contact_id = l.merged_contact_id
    )

select merged_contact_id, surviving_contact_id
from combined
