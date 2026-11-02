{{ config(materialized="view", tblproperties={}) }}

-- Databricks-owned attributes -> EXISTING HubSpot contacts, matched on the contact's
-- HubSpot Record ID (hubspot_contact_id; map it to Record ID on import).
-- Contact-grained companion to company_viability_hubspot; candidacy_hubspot is the
-- feed that creates contacts. New contact attributes we own belong here as columns.
--
-- A diff, not a snapshot: a row per contact where a published value differs from
-- HubSpot's current (Airbyte-ingested) value. The first run after a property is
-- created is therefore its backfill, and later runs carry only changes.
--
-- An unknown on our side (no resolved position, or an unsized serve_priority
-- district) is not sent, so it stays blank in HubSpot rather than being asserted
-- false. A resolved position off the list is a known false and is sent. Exception:
-- a stale HubSpot true is cleared to false so it cannot keep a lead at the top of
-- the outreach queue.
--
-- A view, like the company feed: ops reads it right before an import and wants
-- current values, and both inputs are already materialized. Empty tblproperties:
-- the schema's Delta column mapping is meaningless on a view.
with
    -- HubSpot has no primary-company field and the association array is unordered,
    -- so take the newest associated company by HubSpot createdate, then highest
    -- numeric id (ids are monotonic) as the tie-break and for companies absent
    -- from the ingest. Null when the contact has no associated company. A fallback
    -- import match key for when the contact id fails to match.
    -- a lateral view cannot be followed by a join, so explode in its own CTE
    contact_companies as (
        select ours.id as contact_id, try_cast(cid as bigint) as company_id
        from {{ ref("int__hubspot_contacts") }} as ours
        lateral view explode(ours.companies) as cid
    ),

    ranked_companies as (
        select
            cc.contact_id,
            cc.company_id,
            row_number() over (
                partition by cc.contact_id
                order by co.created_at desc nulls last, cc.company_id desc
            ) as rn
        from contact_companies as cc
        left join
            {{ ref("stg_airbyte_source__hubspot_api_companies") }} as co
            on cc.company_id = co.id
        where cc.company_id is not null
    ),

    newest_company as (
        select contact_id, cast(company_id as string) as hubspot_company_id
        from ranked_companies
        where rn = 1
    ),

    compared as (
        select
            ours.id,
            case
                when ours.is_serve_priority_segment is not null
                then ours.is_serve_priority_segment
                when theirs.serve_priority_segment
                then false
            end as serve_priority_segment,
            theirs.serve_priority_segment as hs_serve_priority_segment,
            newest_company.hubspot_company_id
        from {{ ref("int__hubspot_contacts") }} as ours
        left join newest_company on ours.id = newest_company.contact_id
        -- INNER join: a contact missing from the ingest no longer exists in
        -- HubSpot and would only error on import.
        join
            {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as theirs
            on ours.id = theirs.id
        -- Staging keeps a merged-away contact's last extract looking live, so the
        -- inner join cannot drop it. HubSpot can route a write to its id onto the
        -- survivor, overwriting the survivor's own value.
        left join
            {{ ref("int__hubspot_contact_merges") }} as merged
            on ours.id = merged.merged_contact_id
        where merged.merged_contact_id is null
    )

select
    id as hubspot_contact_id,
    hubspot_company_id,
    serve_priority_segment,
    hs_serve_priority_segment as hubspot_current_serve_priority_segment
from compared
-- equal_null treats a blank HubSpot value as a difference, which is what makes
-- the first run fill every contact we have a value for.
where
    serve_priority_segment is not null
    and not equal_null(serve_priority_segment, hs_serve_priority_segment)
