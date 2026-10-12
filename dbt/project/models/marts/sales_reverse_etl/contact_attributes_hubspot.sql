{{ config(materialized="view", tblproperties={}) }}

-- Databricks-owned attributes -> EXISTING HubSpot contacts, matched on Record ID.
-- Contact-grained companion to company_viability_hubspot; candidacy_hubspot is the
-- feed that creates contacts. New contact attributes we own belong here as columns.
--
-- A diff, not a snapshot: a row per contact where a published value differs from
-- HubSpot's current (Airbyte-ingested) value. The first run after a property is
-- created is therefore its backfill, and later runs carry only changes.
--
-- An unknown on our side (no resolved position, or an unsized district) is not
-- sent, so it stays blank in HubSpot rather than being asserted false. Exception:
-- a stale HubSpot true is cleared to false so it cannot keep a lead at the top of
-- the outreach queue.
--
-- A view, like the company feed: ops reads it right before an import and wants
-- current values, and both inputs are already materialized. Empty tblproperties:
-- the schema's Delta column mapping is meaningless on a view.
with
    compared as (
        select
            ours.id,
            case
                when ours.is_serve_priority_segment is not null
                then ours.is_serve_priority_segment
                when theirs.serve_priority_segment
                then false
            end as serve_priority_segment,
            theirs.serve_priority_segment as hs_serve_priority_segment
        from {{ ref("int__hubspot_contacts") }} as ours
        -- INNER join: a contact missing from the ingest no longer exists in
        -- HubSpot and would only error on import.
        join
            {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as theirs
            on ours.id = theirs.id
    )

select
    id as `Record ID`,
    serve_priority_segment,
    hs_serve_priority_segment as hubspot_current_serve_priority_segment
from compared
-- equal_null treats a blank HubSpot value as a difference, which is what makes
-- the first run fill every contact we have a value for.
where
    serve_priority_segment is not null
    and not equal_null(serve_priority_segment, hs_serve_priority_segment)
