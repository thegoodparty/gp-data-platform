with
    source as (select * from {{ source("amplitude_ingestion", "events_list") }}),
    renamed as (
        select
            -- The new export carries no numeric id; name always equalled value.
            cast(null as decimal(38, 10)) as id,
            value as name,
            value,
            hidden,
            cast(totals as decimal(38, 10)) as totals,
            deleted,
            display,
            cast(null as boolean) as autohidden,
            non_active,
            flow_hidden,
            cast(null as boolean) as in_waitroom,
            cast(null as decimal(38, 10)) as totals_delta,
            cast(null as boolean) as clusters_hidden

        from source
    )
select *
from renamed
