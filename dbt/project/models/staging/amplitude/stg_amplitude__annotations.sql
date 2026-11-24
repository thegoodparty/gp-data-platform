with
    source as (select * from {{ source("amplitude", "annotations") }}),
    renamed as (select id, cast(start as date) as date, label, details from source)
select *
from renamed
