-- Race-level union of DDHQ reported and upcoming races. is_reported flags
-- whether results have been reported by DDHQ for the race. Reported wins
-- on overlap (e.g. the election-night transition window) so the unique
-- test on ddhq_race_id holds.
with
    reported as (
        select *, true as is_reported
        from {{ ref("stg_airbyte_source__ddhq_elections_gsheet_reported_races") }}
    ),

    upcoming as (
        select u.*, false as is_reported
        from {{ ref("stg_airbyte_source__ddhq_elections_gsheet_upcoming_races") }} as u
        left anti join reported as r on u.ddhq_race_id = r.ddhq_race_id
    )

select *
from reported
union all
select *
from upcoming
