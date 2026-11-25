-- The new export is one row per segment per day; fold it back into the JSON
-- object per day that the Airbyte stream delivered.
with
    source as (select * from {{ source("amplitude", "active_users_counts") }}),
    renamed as (
        select
            cast(date as date) as date,
            to_json(
                map_from_entries(collect_list(struct(segment, count)))
            ) as statistics
        from source
        group by 1
    )
select *
from renamed
