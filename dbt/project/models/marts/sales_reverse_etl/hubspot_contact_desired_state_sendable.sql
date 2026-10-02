-- The relation the reverse-ETL app reads. It runs `select * from <relation>` with no
-- row filter and raises on a null, blank, or duplicate key, so held rows must not be
-- here; it also maps every remaining column name onto a HubSpot property, so nothing
-- that is not a property may be here either. Both are why this exists as its own
-- object rather than as a filter the app would have to carry.
--
-- A view, not a table: it is a projection of a table built minutes earlier in the
-- same run, and materializing it again would double the storage and let the two
-- drift within a build.
{{ config(materialized="view") }}

select
    * except (
        contact_exists,
        is_sendable,
        hold_reason,
        gp_candidacy_id,
        gp_candidate_id,
        feed_activity_at,
        built_at
    )
from {{ ref("hubspot_contact_desired_state") }}
where is_sendable
