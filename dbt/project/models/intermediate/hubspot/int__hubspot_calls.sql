{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "calls", "hubspot"],
    )
}}

-- Dedupes staging row inflation (multiple Airbyte syncs per call) and
-- surfaces outcome_family from the hubspot_call_dispositions seed.
with

    deduped_calls as (
        select *
        from {{ ref("stg_airbyte_source__hubspot_api_engagements_calls") }}
        qualify
            row_number() over (partition by id order by _airbyte_extracted_at desc) = 1
    ),

    final as (
        select
            c.*,
            s.disposition_label,
            s.source as disposition_label_source,
            coalesce(
                s.outcome_family,
                case
                    when c.hs_call_disposition is null
                    then 'null_disposition'
                    else 'unmapped'
                end
            ) as outcome_family
        from deduped_calls as c
        left join
            {{ ref("hubspot_call_dispositions") }} as s
            on c.hs_call_disposition = s.guid
    )

select *
from final
