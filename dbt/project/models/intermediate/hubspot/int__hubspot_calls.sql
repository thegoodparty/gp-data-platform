{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "calls", "hubspot"],
    )
}}

select c.*, s.disposition_label, s.source as disposition_label_source
from {{ ref("stg_airbyte_source__hubspot_api_engagements_calls") }} as c
left join {{ ref("hubspot_call_dispositions") }} as s on c.hs_call_disposition = s.guid
