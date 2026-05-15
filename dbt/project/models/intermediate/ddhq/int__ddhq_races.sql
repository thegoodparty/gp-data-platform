-- Race-level union of DDHQ reported and upcoming races. is_reported flags
-- whether results have been reported by DDHQ for the race.
with
    reported as (
        select *, true as is_reported
        from {{ ref("stg_airbyte_source__ddhq_elections_gsheet_reported_races") }}
    ),

    upcoming as (
        select *, false as is_reported
        from {{ ref("stg_airbyte_source__ddhq_elections_gsheet_upcoming_races") }}
    )

select *
from reported
union all
select *
from upcoming
