{{ config(materialized="view") }}

/*
    Call-grain exposure of int__hubspot_calls into mart_analytics (Sigma reads
    marts only). The intermediate is already deduped to one row per call and
    carries the disposition label / outcome_family, so this is a thin
    projection; view, not table, to avoid duplicating that storage.

    hubspot_calls is the call grain; prospects carries the per-contact
    aggregate from int__hubspot_contact_calls. Consumers must not re-derive
    outcome classification from hs_call_disposition GUIDs.
*/
select *
from {{ ref("int__hubspot_calls") }}
