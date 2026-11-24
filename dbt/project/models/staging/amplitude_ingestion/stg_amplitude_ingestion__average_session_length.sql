with
    source as (
        select * from {{ source("amplitude_ingestion", "average_session_length") }}
    ),
    renamed as (
        select
            cast(date as timestamp) as date, cast(length as decimal(38, 10)) as length
        from source
    )
select *
from renamed
